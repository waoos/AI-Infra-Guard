// Package runner 实现运行器
package runner

import (
	"bufio"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Tencent/AI-Infra-Guard/pkg/openai"

	"github.com/Tencent/AI-Infra-Guard/common/fingerprints/parser"
	"github.com/Tencent/AI-Infra-Guard/common/fingerprints/preload"
	"github.com/Tencent/AI-Infra-Guard/common/utils"
	"github.com/Tencent/AI-Infra-Guard/internal/gologger"
	"github.com/Tencent/AI-Infra-Guard/internal/options"
	"github.com/Tencent/AI-Infra-Guard/pkg/httpx"
	"github.com/Tencent/AI-Infra-Guard/pkg/vulstruct"

	"github.com/liushuochen/gotable"
	"github.com/logrusorgru/aurora"
	"github.com/projectdiscovery/fastdialer/fastdialer"
	"github.com/projectdiscovery/hmap/store/hybrid"
	"github.com/remeh/sizedwaitgroup"
	"go.uber.org/ratelimit"

	// automatic fd max increase if running as root
	_ "github.com/projectdiscovery/fdmax/autofdmax"
)

// Runner struct 保存运行指纹扫描所需的所有组件
type Runner struct {
	Options     *options.Options          // 配置选项
	hp          *httpx.HTTPX              // HTTP 客户端
	hm          *hybrid.HybridMap         // 混合存储
	rateLimiter ratelimit.Limiter         // 速率限制器
	result      chan HttpResult           // 结果通道
	fpEngine    *preload.Runner           // 指纹引擎
	advEngine   *vulstruct.AdvisoryEngine // 漏洞建议引擎
	total       int                       // 总目标数
	done        chan struct{}             // 用于优雅关闭的通道
}

// New 初始化一个新的 Runner 实例
func New(options2 *options.Options) (*Runner, error) {
	runner := &Runner{
		Options: options2,
		total:   0,
		done:    make(chan struct{}), // 初始化done通道用于优雅关闭
	}

	// 依次初始化各个组件
	if err := runner.initStorage(); err != nil {
		return nil, err
	}

	if err := runner.processTargets(); err != nil {
		return nil, err
	}

	if err := runner.initComponents(); err != nil {
		return nil, err
	}

	if err := runner.initFingerprints(); err != nil {
		return nil, err
	}

	if err := runner.initVulnerabilityDB(); err != nil {
		return nil, err
	}

	return runner, nil
}

// initFingerprints initializes the fingerprint detection engine
func (r *Runner) initFingerprints() error {
	options2 := r.Options
	// 初始化指纹
	if !utils.IsFileExists(options2.FPTemplates) {
		gologger.Fatalf("没有指定指纹模板文件:%s", options2.FPTemplates)
	}
	fps := make([]parser.FingerPrint, 0)
	if utils.IsDir(options2.FPTemplates) {
		files, err := utils.ScanDir(options2.FPTemplates)
		if err != nil {
			gologger.Fatalf("无法扫描指纹模板目录:%s", options2.FPTemplates)
		}
		for _, filename := range files {
			if !strings.HasSuffix(filename, ".yaml") {
				continue
			}
			data, err := os.ReadFile(filename)
			if err != nil {
				gologger.Fatalf("无法读取指纹模板文件:%s", filename)
			}
			fp, err := parser.InitFingerPrintFromData(data)
			if err != nil {
				gologger.WithError(err).Fatalf("无法解析指纹模板文件:%s", filename)
			}
			fps = append(fps, *fp)
		}
	} else {
		data, err := os.ReadFile(options2.FPTemplates)
		if err != nil {
			gologger.Fatalf("无法读取指纹模板文件:%s", options2.FPTemplates)
		}
		fp, err := parser.InitFingerPrintFromData(data)
		if err != nil {
			gologger.Fatalf("无法解析指纹模板文件:%s", options2.FPTemplates)
		}
		fps = append(fps, *fp)
	}
	if len(fps) == 0 {
		gologger.Fatalf("没有指定指纹模板")
	}
	r.fpEngine = preload.New(r.hp, fps)
	gologger.Infof("加载指纹库,数量:%d", len(fps)+len(preload.CollectedFpReqs()))
	r.result = make(chan HttpResult)
	return nil
}

// initStorage 初始化混合存储
func (r *Runner) initStorage() error {
	hm, err := hybrid.New(hybrid.DefaultDiskOptions)
	if err != nil {
		return fmt.Errorf("could not create temporary input file: %s", err)
	}
	r.hm = hm
	return nil
}

// processTargetList 处理目标列表
// 支持处理CIDR格式的IP段和单个目标
func (r *Runner) processTargetList(targets []string) {
	for _, t := range targets {
		if utils.IsCIDR(t) {
			// 处理CIDR格式
			cidrIps, err := IPAddresses(t)
			if err != nil {
				r.hm.Set(t, nil)
				r.total++
			} else {
				// 展开CIDR中的所有IP
				for _, ip := range cidrIps {
					r.hm.Set(ip, nil)
					r.total++
				}
			}
		} else {
			// 处理单个目标
			r.hm.Set(t, nil)
			r.total++
		}
	}
}

// processTargets 处理所有输入的目标
// 支持从命令行参数和文件读取目标
func (r *Runner) processTargets() error {
	// 处理命令行指定的目标
	if r.Options.Target != nil {
		r.processTargetList(r.Options.Target)
	}

	// 处理目标文件
	if r.Options.TargetFile != "" {
		if utils.IsFileExists(r.Options.TargetFile) {
			file, err := os.Open(r.Options.TargetFile)
			if err != nil {
				return err
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			targets := make([]string, 0)
			for scanner.Scan() {
				t := strings.TrimSpace(scanner.Text())
				if t != "" {
					targets = append(targets, t)
				}
			}
			r.processTargetList(targets)
		}
	}

	if r.Options.LocalScan {
		op, err := utils.GetLocalOpenPorts()
		if err != nil {
			gologger.Fatalf("get local open port failed,err:%s", err)
		}
		var targets []string
		for _, p := range op {
			targets = append(targets, p.Address+":"+strconv.Itoa(p.Port))
		}
		r.processTargetList(targets)
	}
	if r.total > 0 {
		gologger.Infof("加载目标数量:%d", r.total)
	}
	return nil
}

// initComponents 初始化基础组件
// 包括速率限制器、HTTP客户端等
func (r *Runner) initComponents() error {
	// 初始化速率限制器
	r.rateLimiter = ratelimit.New(r.Options.RateLimit)
	r.result = make(chan HttpResult)

	// 初始化DNS解析器
	dialer, err := fastdialer.NewDialer(fastdialer.DefaultOptions)
	if err != nil {
		return fmt.Errorf("could not create resolver cache: %s", err)
	}

	// 配置HTTP客户端选项
	httpOptions := &httpx.HTTPOptions{
		Timeout:          time.Duration(r.Options.TimeOut) * time.Second,
		RetryMax:         1,
		FollowRedirects:  false,
		HTTPProxy:        r.Options.ProxyURL,
		Unsafe:           false,
		DefaultUserAgent: httpx.GetRandomUserAgent(),
		Dialer:           dialer,
		CustomHeaders:    r.Options.Headers,
	}

	// 创建HTTP客户端
	hp, err := httpx.NewHttpx(httpOptions)
	if err != nil {
		return err
	}
	r.hp = hp
	return nil
}

// extractContent 处理 HTTP 响应并提取相关信息
func (r *Runner) extractContent(fullUrl string, resp *httpx.Response, respTime string) {
	builder := &strings.Builder{}
	builder.WriteString(fullUrl)

	builder.WriteString(" [")
	// 根据状态码设置不同颜色
	switch {
	case resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices:
		builder.WriteString(aurora.Green(strconv.Itoa(resp.StatusCode)).String()) // 2xx 绿色
	case resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest:
		builder.WriteString(aurora.Yellow(strconv.Itoa(resp.StatusCode)).String()) // 3xx 黄色
	case resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError:
		builder.WriteString(aurora.Red(strconv.Itoa(resp.StatusCode)).String()) // 4xx 红色
	case resp.StatusCode > http.StatusInternalServerError:
		builder.WriteString(aurora.Bold(aurora.Yellow(strconv.Itoa(resp.StatusCode))).String()) // 5xx 加粗黄色
	}
	builder.WriteString("] ")
	// 检测是否跳转,跳转则转过去，新建一个结果
	if resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest {
		newUrl := resp.GetHeader("Location")
		_ = r.runDomainRequest(newUrl)
	}

	title := resp.Title
	builder.WriteString(" [")
	builder.WriteString(title)
	builder.WriteString("] ")

	iconData, err := utils.GetFaviconBytes(r.hp, fullUrl, resp.Data)
	faviconHash := utils.FaviconHash(iconData)
	if err != nil {
		faviconHash = 0
	}
	// 内部指纹
	fpResults := r.fpEngine.RunFpReqs(fullUrl, 10, faviconHash)
	ads := make([]vulstruct.VersionVul, 0)
	isInternal := true
	if strings.Contains(fullUrl, "127.0.0.1") {
		isInternal = false
	}
	if strings.Contains(fullUrl, "localhost") {
		isInternal = false
	}
	if len(fpResults) > 0 {
		for _, item := range fpResults {
			builder.WriteString("[")
			builder.WriteString(item.Name)
			if item.Type != "" {
				builder.WriteString(":")
				builder.WriteString(item.Type)
			}
			if item.Version != "" {
				builder.WriteString(":")
				builder.WriteString(item.Version)
			}
			builder.WriteString("]")
			builder.WriteString(" ")

			advisories, err := r.advEngine.GetAdvisories(item.Name, item.Version, isInternal)
			if err != nil {
				gologger.Errorf("get advisory error: %s", err)
			} else {
				// 添加漏洞信息
				ads = append(ads, advisories...)
			}
			builder.WriteString(" ")
		}
	}

	result := HttpResult{
		URL:           fullUrl,
		Title:         title,
		ContentLength: resp.ContentLength,
		StatusCode:    resp.StatusCode,
		ResponseTime:  respTime,
		Fingers:       fpResults,
		s:             builder.String(),
		Advisories:    ads,
	}
	r.result <- result
}

// runHostRequest 尝试使用 HTTP 和 HTTPS 连接到主机
func (r *Runner) runHostRequest(domain string) {
	retried := false
	protocol := httpx.HTTP
retry:
	fullUrl := fmt.Sprintf("%s://%s", protocol, domain)
	timeStart := time.Now()
	resp, err := r.hp.Get(fullUrl, nil)
	if err != nil {
		if !retried {
			if protocol == httpx.HTTP {
				protocol = httpx.HTTPS
			} else {
				protocol = httpx.HTTP
			}
			retried = true
			goto retry
		}
		return
	}
	r.extractContent(fullUrl, resp, time.Since(timeStart).String())
}

// runDomainRequest makes a request to a specific URL and processes the response
func (r *Runner) runDomainRequest(fullUrl string) error {
	timeStart := time.Now()
	resp, err := r.hp.Get(fullUrl, nil)
	if err != nil {
		return err
	}
	r.extractContent(fullUrl, resp, time.Since(timeStart).String())
	return nil
}

// Close cleans up resources used by the Runner
func (r *Runner) Close() {
	r.hp.Options.Dialer.Close()
	_ = r.hm.Close()
}

func (r *Runner) callbackProcess(current, total int) {
	if r.Options.Callback != nil {
		r.Options.Callback(CallbackProcessInfo{
			Current: current,
			Total:   total,
		})
	}
}

// RunEnumeration 开始扫描所有目标
func (r *Runner) RunEnumeration() {
	// 检查是否有输入目标
	if r.total == 0 {
		gologger.Fatalf("没有指定输入，输入 -h 查看帮助")
		return
	}
	r.callbackProcess(0, r.total)

	// 启动输出处理协程
	outputWg := sizedwaitgroup.New(1)
	outputWg.Add()
	go r.handleOutput(&outputWg)

	timeStart := time.Now()
	wg := sizedwaitgroup.New(r.Options.RateLimit)
	var numTarget uint64 = 0

	r.hm.Scan(func(k, _ []byte) error {
		wg.Add()
		target := string(k)
		if !strings.HasPrefix(target, "http") {
			go func() {
				defer wg.Done()
				r.rateLimiter.Take()
				r.runHostRequest(target)
				atomic.AddUint64(&numTarget, 1)
				r.callbackProcess(int(atomic.LoadUint64(&numTarget)), r.total)
			}()
		} else {
			go func() {
				defer wg.Done()
				r.rateLimiter.Take()
				r.runDomainRequest(target)
				atomic.AddUint64(&numTarget, 1)
				r.callbackProcess(int(atomic.LoadUint64(&numTarget)), r.total)
			}()
		}
		return nil
	})
	wg.Wait()
	close(r.result)
	outputWg.Wait()
	duration := time.Since(timeStart)
	gologger.Infof("扫描完成～耗时:%s", utils.Duration2String(duration))
}

// handleOutput 处理扫描结果的输出
func (r *Runner) handleOutput(wg *sizedwaitgroup.SizedWaitGroup) {
	defer wg.Done()

	f, err := r.createOutputFile()
	if err != nil {
		gologger.Fatalf("创建输出文件失败: %v", err)
		return
	}
	if f != nil {
		defer f.Close()
	}
	var results []HttpResult
	for result := range r.result {
		results = append(results, result)
		r.writeResult(f, result)
	}
	// summary table
	if len(results) > 0 {
		table, err := gotable.Create("Target", "StatusCode", "Title", "FingerPrint")
		if err != nil {
			gologger.Errorf("create table error: %v", err)
			return
		}
		vulTable, err := gotable.Create("CVE", "Severity", "VulName", "Target", "Suggestions")
		if err != nil {
			gologger.Errorf("create table error:%v", err)
			return
		}
		var showVulTable bool = false
		for _, row := range results {
			data := make(map[string]string)
			var fpString string = ""
			for _, fp := range row.Fingers {
				fpString += fp.Name
				if fp.Type != "" {
					fpString += ":" + fp.Type
				}
				if fp.Version != "" {
					fpString += ":" + fp.Version
				}
			}
			data = map[string]string{
				"Target":      row.URL,
				"StatusCode":  fmt.Sprintf("%d", row.StatusCode),
				"Title":       row.Title,
				"FingerPrint": fpString,
			}
			table.AddRow(data)

			// write into vulTable
			for _, ad := range row.Advisories {
				showVulTable = true
				var adRow = []string{
					ad.Info.CVEName,
					ad.Info.Severity,
					ad.Info.Summary,
					row.URL,
					ad.Info.SecurityAdvise,
				}
				vulTable.AddRow(adRow)
			}
		}
		fmt.Println("Application Summary:")
		fmt.Println(table.String())
		if showVulTable {
			fmt.Println("Vulnerability Summary:")
			fmt.Println(vulTable.String())
		}
	}

	if r.Options.Callback != nil {
		score := r.calcSecScore(results)
		r.Options.Callback(score)
	}
}

// createOutputFile 创建输出文件
func (r *Runner) createOutputFile() (*os.File, error) {
	if r.Options.Output == "" {
		return nil, nil
	}
	return os.OpenFile(r.Options.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
}

// writeResult 写入扫描结果
func (r *Runner) writeResult(f *os.File, result HttpResult) {
	fmt.Println(result.s)
	if f != nil {
		_, _ = f.WriteString(result.s + "\n")
	}
	if r.Options.Callback != nil {
		vuls := make([]vulstruct.Info, 0)
		for _, item := range result.Advisories {
			vuls = append(vuls, item.Info)
		}
		var fpString string = ""
		for _, fp := range result.Fingers {
			fpString += fp.Name
			if fp.Type != "" {
				fpString += ":" + fp.Type
			}
			if fp.Version != "" {
				fpString += ":" + fp.Version
			}
		}
		if r.Options.Callback != nil {
			r.Options.Callback(CallbackScanResult{
				TargetURL:       result.URL,
				StatusCode:      result.StatusCode,
				Title:           result.Title,
				Fingerprint:     fpString,
				Vulnerabilities: vuls,
			})
		}
	}
	if len(result.Advisories) > 0 {
		fmt.Println("\n存在漏洞:")
		for _, item := range result.Advisories {
			builder := strings.Builder{}
			builderFile := strings.Builder{}
			serverity := item.Info.Severity
			name := item.Info.CVEName
			if serverity == "HIGH" || serverity == "CRITICAL" {
				builder.WriteString(aurora.Red(fmt.Sprintf("%s [%s]", name, serverity)).String()) // 高危红色
			} else if serverity == "MEDIUM" {
				builder.WriteString(aurora.Yellow(fmt.Sprintf("%s [%s]", name, serverity)).String()) // 中危黄色
			} else {
				builder.WriteString(aurora.Bold(fmt.Sprintf("%s [%s]", name, serverity)).String()) // 低危加粗
			}
			builderFile.WriteString(fmt.Sprintf("%s [%s]\n", name, serverity))
			builder.WriteString(": " + item.Info.Summary + "\n" + item.Info.Details + "\n")
			builderFile.WriteString(": " + item.Info.Summary + "\n" + item.Info.Details + "\n")
			if len(item.Info.SecurityAdvise) > 0 {
				builder.WriteString("修复建议: " + item.Info.SecurityAdvise + "\n")
				builderFile.WriteString("修复建议: " + item.Info.SecurityAdvise + "\n")
			}
			fmt.Println(builder.String())
			_, _ = f.WriteString(builderFile.String() + "\n")
		}
		if r.Options.AIAnalysis {
			fmt.Println("AI分析:")
			prompt := "你是安全漏洞报告解读大师，我会给你扫描器输出的url和存在的cve详情。以编写甲方漏洞报告的形式编写完整报告，参考格式如：\n# 一、风险总览\n(描述测试的url以及基本信息，综合CVE漏洞可能造成的严重漏洞后果)\n# 二、漏洞详情\n(请你利用搜索等功能，依次分析CVE的详情，给出漏洞怎么产生，怎么利用，修复方案的详情(根据漏洞类型给出对应修复方案，执行的命令,而不是简单升级)，然后给出可靠参考来源,相同类型漏洞合并在一起给出)\n漏洞报告如下：\n"
			prompt += fmt.Sprintf("%s title:%s fingerprint:%v", result.URL, result.Title, result.Fingers) + "\n"
			for _, item := range result.Advisories {
				prompt += fmt.Sprintf("%s[%s]:%s\n", item.Info.CVEName, item.Info.Severity, item.Info.Details)
				prompt += fmt.Sprintf("reference: %v\n", item.References)
			}
			var err error
			var full string
			if r.Options.AIDeepSeekToken != "" {
				full, err = openai.DeepSeekR1API(prompt, r.Options.AIDeepSeekToken)
			} else {
				full, err = openai.HunyuanAI(prompt, r.Options.AIHunyuanToken)
			}
			if err != nil {
				gologger.WithError(err).Errorln("AI分析失败")
			}
			_, _ = f.WriteString(full + "\n")
		}
	}
}

// GetFpAndVulList 获取指纹和漏洞列表
func (r *Runner) GetFpAndVulList() []FpInfos {
	fingerprints := make([]parser.FingerPrint, 0)
	for _, fp := range r.fpEngine.GetFps() {
		fp2 := fp
		fingerprints = append(fingerprints, fp2)
	}

	fps := make([]FpInfos, 0)
	for _, fp := range fingerprints {
		ads, err := r.advEngine.GetAdvisories(fp.Info.Name, "", false)
		if err != nil {
			gologger.WithError(err).Errorln("获取漏洞列表失败", fp)
			continue
		}
		fps = append(fps, FpInfos{
			FpName: fp.Info.Name,
			Vuls:   ads,
			Desc:   fp.Info.Desc,
		})
	}
	return fps
}

// ShowFpAndVulList displays the list of available fingerprints and vulnerabilities
// 显示指纹和漏洞列表
func (r *Runner) ShowFpAndVulList(vul bool) {
	data := r.GetFpAndVulList()
	if vul {
		gologger.Infoln("漏洞列表:")
		table, err := gotable.Create("组件名称", "组件简介", "漏洞数量")
		if err != nil {
			gologger.Errorf("create table error: %v", err)
			return
		}
		for _, item := range data {
			table.AddRow([]string{item.FpName, item.Desc, strconv.Itoa(len(item.Vuls))})
		}
		fmt.Println(table)
	}
}

// initVulnerabilityDB initializes the vulnerability advisory engine
func (r *Runner) initVulnerabilityDB() error {
	vulDir := strings.TrimRight(r.Options.AdvTemplates, "/")
	if r.Options.Language == "en" {
		vulDir = vulDir + "_en"
	}
	engine, err := vulstruct.NewAdvisoryEngine(vulDir)
	if err != nil {
		gologger.Fatalf("无法初始化漏洞库:%s", err)
	}
	r.advEngine = engine
	gologger.Infof("加载漏洞版本库,数量:%d", r.advEngine.GetCount())
	return nil
}

// calcSecScore 计算安全分数
func (r *Runner) calcSecScore(results []HttpResult) CallbackReportInfo {
	var total, high, middle, low int = 0, 0, 0, 0
	for _, result := range results {
		total += len(result.Advisories)
		for _, item := range result.Advisories {
			if item.Info.Severity == "HIGH" || item.Info.Severity == "CRITICAL" {
				high++
			} else if item.Info.Severity == "MEDIUM" {
				middle++
			} else {
				low++
			}
		}
	}
	if len(results) == 0 && total == 0 {
		return CallbackReportInfo{
			SecScore:   0,
			HighRisk:   0,
			MediumRisk: 0,
			LowRisk:    0,
		}
	} else if total == 0 {
		return CallbackReportInfo{
			SecScore:   100,
			HighRisk:   0,
			MediumRisk: 0,
			LowRisk:    0,
		}
	}
	// 计算加权风险比例
	weightedRisk := (float64(high)/float64(total))*0.7 +
		(float64(middle)/float64(total))*0.5 +
		(float64(low)/float64(total))*0.3

	// 计算安全评分（百分制）
	safetyScore := 100 - weightedRisk*100

	// 确保评分在0-100范围内
	if safetyScore < 0 {
		safetyScore = 0
	}
	if safetyScore >= 100 {
		safetyScore = 100
	}

	ret := CallbackReportInfo{
		SecScore:   int(math.Round(safetyScore)),
		HighRisk:   high,
		MediumRisk: middle,
		LowRisk:    low,
	}
	return ret
}
