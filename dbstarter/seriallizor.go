package dbstarter

import (
	"errors"

	"gorm.io/gorm"
)

const DefaultPageIndex = 1
const DefaultPageSize = 20

type QueryData struct {
	Pagination bool
	PageIndex  int
	PageSize   int
	Search     string
	Filter     map[string]interface{}
}

// 将map转化为QueryData
func (query *QueryData) Init(data map[string]interface{}) {
	if query.Filter == nil {
		query.Filter = map[string]interface{}{}
	}
	for key, valI := range data {
		switch key {
		case "pagination":
			query.Pagination, _ = valI.(bool)
		case "page_index":
			query.PageIndex, _ = valI.(int)
		case "page_size":
			query.PageSize, _ = valI.(int)
		case "search":
			query.Search, _ = valI.(string)
		default:
			query.Filter[key] = valI
		}
	}
}

type Serializor struct {
	ListFields   []string // 空表示全部字段
	DetailFields []string // 空表示全部字段
	SearchFields []string // 空表示不支持按字段模糊搜索
	FilterFields []string
	OrderBy      string
	Query        *QueryData
}

func (slz *Serializor) ListQuery(table string, queryData map[string]interface{}) (int64, *gorm.DB, error) {
	// 检查数据库连接状态
	if DB == nil {
		return 0, nil, errors.New("db is not initialized")
	}

	// 解析查询数据
	query := &QueryData{}
	if queryData != nil {
		query.Init(queryData)
	} else if slz.Query != nil {
		query = slz.Query
	}

	// dbtx := DB.Table(table).Debug().Where("deleted_at is NULL")
	dbtx := DB.Table(table).Where("id > 0")

	// select field
	if len(slz.ListFields) > 0 {
		dbtx = dbtx.Select(slz.ListFields)
	}

	// filter
	if len(query.Filter) > 0 {
		dbtx = dbtx.Where(query.Filter)
	}

	// search (implicitly)
	if query.Search != "" && len(slz.SearchFields) > 0 {
		search_str := "%" + query.Search + "%"
		searchTx := DB.Where(slz.SearchFields[0]+" LIKE ?", search_str)
		for _, field := range slz.SearchFields[1:] {
			searchTx = searchTx.Or(field+" LIKE ?", search_str)
		}
		dbtx.Where(searchTx)
	}

	// order
	if slz.OrderBy != "" {
		dbtx = dbtx.Order(slz.OrderBy)
	}

	// totalSize by condition
	var totalSize int64
	dbtx = dbtx.Count(&totalSize)
	if err := dbtx.Error; err != nil {
		return 0, nil, err
	}

	// pagination
	if query.Pagination {
		if query.PageIndex <= 0 || query.PageSize <= 0 {
			query.PageIndex = DefaultPageIndex
			query.PageSize = DefaultPageSize
		}
		offset := (query.PageIndex - 1) * query.PageSize
		dbtx = dbtx.Offset(offset).Limit(query.PageSize)
	}

	return totalSize, dbtx, nil
}
