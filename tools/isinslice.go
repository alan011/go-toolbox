package tools

func IsStrInSlice(val string, container []string) bool {
	for _, item := range container {
		if item == val {
			return true
		}
	}
	return false

}

func IsInterfaceInSlice(val interface{}, container []interface{}) bool {
	for _, item := range container {
		if item == val {
			return true
		}
	}
	return false

}
