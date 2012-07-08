package main

import "github.com/xiam/gosexy/yaml"

func main() {
	settings := yaml.New()
	defer settings.Write("test.yaml")
	settings.Set("success", true)
}
