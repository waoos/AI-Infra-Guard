package websocket

// 本文件用于实现知识库（指纹库、漏洞库）相关的API接口。
// 后续可在此文件中实现指纹库、漏洞库的增删改查、分页、条件查询等接口。

// TODO: 实现指纹库和漏洞库相关的API接口。

// 替换为实际指纹结构体路径

import (
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Tencent/AI-Infra-Guard/common/fingerprints/parser"
	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

func HandleListFingerprints(c *gin.Context) {
	// 1. 解析分页参数
	pageStr := c.DefaultQuery("page", "1")
	sizeStr := c.DefaultQuery("size", "10")
	page, _ := strconv.Atoi(pageStr)
	size, _ := strconv.Atoi(sizeStr)
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 10
	}

	// 2. 获取查询参数
	nameQuery := strings.ToLower(c.DefaultQuery("name", ""))
	// severityQuery := strings.ToLower(c.DefaultQuery("severity", ""))
	// categoryQuery := strings.ToLower(c.DefaultQuery("category", ""))

	// 3. 读取 data/fingerprints/ 下所有分类和YAML文件
	var allFingerprints []parser.FingerPrint
	root := "data/fingerprints"
	files, _ := os.ReadDir(root)
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".yaml") {
			content, _ := os.ReadFile(filepath.Join(root, f.Name()))
			fp, err := parser.InitFingerPrintFromData(content)
			if err == nil && fp != nil {
				allFingerprints = append(allFingerprints, *fp)
			}
		}
	}

	// 再读取所有子目录下的 .yaml 文件
	for _, cat := range files {
		if cat.IsDir() {
			subFiles, _ := os.ReadDir(filepath.Join(root, cat.Name()))
			for _, f := range subFiles {
				if strings.HasSuffix(f.Name(), ".yaml") {
					content, _ := os.ReadFile(filepath.Join(root, cat.Name(), f.Name()))
					fp, err := parser.InitFingerPrintFromData(content)
					if err == nil && fp != nil {
						allFingerprints = append(allFingerprints, *fp)
					}
				}
			}
		}
	}

	// 4. 条件过滤
	var filteredFingerprints []parser.FingerPrint
	for _, fp := range allFingerprints {
		// name模糊匹配
		if nameQuery != "" && !strings.Contains(strings.ToLower(fp.Info.Name), nameQuery) {
			continue
		}
		// severity等值匹配
		// if severityQuery != "" && strings.ToLower(fp.Info.Severity) != severityQuery {
		// 	continue
		// }
		// // category等值匹配（在metadata中查找）
		// if categoryQuery != "" {
		// 	cat, ok := fp.Info.Metadata["category"]
		// 	if !ok || strings.ToLower(cat) != categoryQuery {
		// 		continue
		// 	}
		// }
		filteredFingerprints = append(filteredFingerprints, fp)
	}

	// 5. 分页
	total := len(filteredFingerprints)
	start := (page - 1) * size
	end := start + size
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	items := filteredFingerprints[start:end]

	// 6. 返回
	c.JSON(http.StatusOK, gin.H{
		"status":  0,
		"message": "success",
		"data": gin.H{
			"total": total,
			"page":  page,
			"size":  size,
			"items": items,
		},
	})
}

func HandleCreateFingerprint(c *gin.Context) {
	// 1. 解析请求体为 parser.FingerPrint
	var fp parser.FingerPrint
	if err := c.ShouldBindJSON(&fp); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": 1, "message": "参数解析失败", "data": nil})
		return
	}

	// 2. 检查指纹名称是否已存在（可选，防止重复）
	yamlPath := filepath.Join("data/fingerprints", fp.Info.Name+".yaml")
	if _, err := os.Stat(yamlPath); err == nil {
		c.JSON(http.StatusConflict, gin.H{"status": 1, "message": "指纹已存在", "data": nil})
		return
	}

	// 3. 序列化为YAML并写入文件
	yamlData, err := yaml.Marshal(fp)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": 1, "message": "YAML序列化失败", "data": nil})
		return
	}
	if err := os.WriteFile(yamlPath, yamlData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": 1, "message": "文件写入失败", "data": nil})
		return
	}

	// 4. 返回成功
	c.JSON(http.StatusOK, gin.H{"status": 0, "message": "success", "data": fp})
}

// 批量删除指纹请求体
type BatchDeleteRequest struct {
	Names []string `json:"names"`
}

func HandleDeleteFingerprint(c *gin.Context) {
	var req BatchDeleteRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.Names) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"status": 1, "message": "参数错误", "data": nil})
		return
	}

	var deleted []string
	var notFound []string

	for _, name := range req.Names {
		yamlPath := filepath.Join("data/fingerprints", name+".yaml")
		if _, err := os.Stat(yamlPath); os.IsNotExist(err) {
			notFound = append(notFound, name)
			continue
		}
		if err := os.Remove(yamlPath); err == nil {
			deleted = append(deleted, name)
		}
	}

	msg := "删除完成"
	if len(notFound) > 0 {
		msg += "，部分指纹未找到: " + strings.Join(notFound, ", ")
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  0,
		"message": msg,
		"data": gin.H{
			"deleted":  deleted,
			"notFound": notFound,
		},
	})
}

// 编辑指纹处理函数
func HandleEditFingerprint(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": 1, "message": "指纹名称不能为空", "data": nil})
		return
	}

	var fp parser.FingerPrint
	if err := c.ShouldBindJSON(&fp); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": 1, "message": "参数解析失败", "data": nil})
		return
	}

	newName := fp.Info.Name
	oldPath := filepath.Join("data/fingerprints", name+".yaml")
	newPath := filepath.Join("data/fingerprints", newName+".yaml")

	// 检查原文件是否存在
	if _, err := os.Stat(oldPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"status": 1, "message": "原指纹不存在", "data": nil})
		return
	}

	// 如果新名字和旧名字不同，且新名字已存在，报冲突
	if newName != name {
		if _, err := os.Stat(newPath); err == nil {
			c.JSON(http.StatusConflict, gin.H{"status": 1, "message": "新指纹名称已存在", "data": nil})
			return
		}
	}

	// 删除原文件
	if err := os.Remove(oldPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": 1, "message": "删除原文件失败", "data": nil})
		return
	}

	// 写入新文件
	yamlData, err := yaml.Marshal(fp)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": 1, "message": "YAML序列化失败", "data": nil})
		return
	}
	if err := os.WriteFile(newPath, yamlData, 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": 1, "message": "写入新文件失败", "data": nil})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": 0, "message": "success", "data": fp})
}
