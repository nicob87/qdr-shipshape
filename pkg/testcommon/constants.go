package testcommon

const (
	TimeoutSetupTeardown = 20
	PropertyImageQPIDDispatch = "image.qpid-dispatch"
)

func GetEnvProperty(key string, defaultValue string) string {

	if key == "" {
		return defaultValue
	}

	// TODO Implement lookup mechanism to search for given key on
	//      environment and properties files.
	return defaultValue
}
