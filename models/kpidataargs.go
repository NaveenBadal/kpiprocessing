package models

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type CustomTime struct {
	time.Time
}

func (ct *CustomTime) UnmarshalJSON(b []byte) error {
	var timeStr string
	if err := json.Unmarshal(b, &timeStr); err != nil {
		return err
	}

	if strings.Contains(timeStr, "Z") {
		t, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return fmt.Errorf("failed to parse RFC 3339 time %q: %v", timeStr, err)
		}
		ct.Time = t
	} else {
		const layout = "2006-01-02T15:04:05.9999999"
		t, err := time.Parse(layout, timeStr)
		if err != nil {
			return fmt.Errorf("failed to parse time %q: %v", timeStr, err)
		}
		ct.Time = t.In(time.Local)
	}

	return nil
}

type NetworkMode string

const (
	None  NetworkMode = "None"
	LTE   NetworkMode = "LTE"
	WCDMA NetworkMode = "WCDMA"
	NR    NetworkMode = "NR"
	GSM   NetworkMode = "GSM"
)

type MessageType struct {
	IsL3Message         bool        `json:"IsL3Message"`
	NetworkMode         NetworkMode `json:"NetworkMode"`
	Direction           string      `json:"Direction"`
	Message             string      `json:"Message"`
	DecodedMessage      string      `json:"DecodedMessage"`
	RawDecodedMessage   string      `json:"RawDecodedMessage"`
	Channel             string      `json:"Channel"`
	ChannelType         string      `json:"ChannelType"`
	RowOctate           string      `json:"RowOctate"`
	DecodedMessageXElem string      `json:"DecodedMessage_XElement"`
}

type KPIDataArgs struct {
	SimId                       int                    `json:"SimId"`
	KPIGroupName                string                 `json:"KPI_GroupName"`
	DeviceId                    int                    `json:"DeviceId"`
	Time                        CustomTime             `json:"Time"`
	KPIVals                     map[string]interface{} `json:"KPI_Vals"`
	KPIsNotToRoundOff           []string               `json:"KPIsNotToRoundOff"`
	AdditionalInfo              string                 `json:"AdditionalInfo"`
	ProcessChildKPIGroups       bool                   `json:"ProcessChildKPIGroups"`
	IsCSVRaise                  bool                   `json:"IsCSVRaise"`
	MessageType                 MessageType            `json:"MessageType"`
	IsRaiseFromAzmCalculation   bool                   `json:"IsRaiseFromAzmCalculation"`
	ScriptTableName             string                 `json:"ScriptTableName"`
	KpiDisplayNameToKpiNameDict map[string]string      `json:"KpiDisplayNameToKpiNameDict"`
}
