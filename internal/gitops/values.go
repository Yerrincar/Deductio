package gitops

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

const ValuesFilePath = "./gitops-files/values.yaml"

type ValuesFile struct {
	Applications map[string]ApplicationValues `yaml:"applications"`
}

type ApplicationValues struct {
	Image ImageValues `yaml:"image"`
}

type ImageValues struct {
	Repository string `yaml:"repository"`
	Tag        string `yaml:"tag"`
}

func UpdateApplicationVersion(filePath, application, version string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read values file: %w", err)
	}

	var values ValuesFile
	if err := yaml.Unmarshal(data, &values); err != nil {
		return fmt.Errorf("unmarshal values file: %w", err)
	}

	appValues, ok := values.Applications[application]
	if !ok {
		return fmt.Errorf("application %q not found in values file", application)
	}

	appValues.Image.Tag = version
	values.Applications[application] = appValues

	updatedData, err := yaml.Marshal(values)
	if err != nil {
		return fmt.Errorf("marshal values file: %w", err)
	}

	if err := os.WriteFile(filePath, updatedData, 0644); err != nil {
		return fmt.Errorf("write values file: %w", err)
	}

	return nil
}
