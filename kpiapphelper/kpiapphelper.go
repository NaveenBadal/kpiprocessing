package kpiapphelper

import "strings"

type KpiAppArgs struct {
	Files []string
}

//creating kpiappargs constructor
//need to mention the input files here
func NewKpiAppArgs() *KpiAppArgs {
	return &KpiAppArgs{
		Files: []string{
			"./logs/sample.msig",
		},
	}
}

func (kpiAppArgs *KpiAppArgs) GetInputFiles() string {
	return strings.Join(kpiAppArgs.Files, ";")
}

func (k *KpiAppArgs) GetCommandLine() (string, []string) {
	args := []string{
		"./kpiapp/SigmaPA.Kpi.App.dll",
		"kpiprocess",
		"-f",
		k.GetInputFiles(),
		"--noninteractive",
	}
	return "dotnet", args
}
