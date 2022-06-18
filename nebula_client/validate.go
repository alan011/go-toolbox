package nebula_client

import "errors"

// 用于校验tagName，edgeTypeName，vid，等这些命名的字符串是否合法。
// 参数toCheckUniq，用于检查是否其直全局唯一。
func ValidateName(name string) error {
	if name == "" {
		return errors.New("invalid vid")
	}
	return nil
}

func ValidateVertex(tag string, schema map[string]string, data map[string]interface{}) error {
	// 校验vid
	vid, _ := data["vid"].(string)
	if err := ValidateName(vid); err != nil {
		return err
	}

	return nil
}

func ValidateEdge(edge string, schema map[string]string, data map[string]interface{}) error {
	// 校验vid
	src_vid, _ := data["src_vid"].(string)
	if ValidateName(src_vid) != nil {
		return errors.New("invalid src_vid for edge")
	}
	dst_vid, _ := data["dst_vid"].(string)
	if ValidateName(dst_vid) != nil {
		return errors.New("invalid dst_vid for edge")
	}
	return nil
}
