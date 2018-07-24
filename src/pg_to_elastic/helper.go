package pg_to_elastic

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
)

func getString(i interface{}) (s string) {
	s, _ = i.(string)
	return
}

func getUint(i interface{}) (s uint32) {
	switch t := i.(type) {
	case string:
		{
			integer, _ := strconv.Atoi(t)
			s = uint32(integer)
			return
		}
	case float64:
		{
			s = uint32(t)
			return
		}
	case int:
		{
			s = uint32(t)
			return
		}
	case float32:
		{
			s = uint32(t)
			return
		}
	}
	return
}

func getStringP(i interface{}) (s string, ok bool) {
	ok = true
	switch t := i.(type) {
	case string:
		{
			s = t
			return
		}
	case float64:
		{
			s = fmt.Sprint(t)
			return
		}
	case int:
		{
			s = strconv.Itoa(t)
			return
		}
	case float32:
		{
			s = fmt.Sprint(t) //strconv.FormatFloat(float64(t), 'f', 2, 64)
			return
		}
	}
	ok = false
	return
}

func getUintFromFloat64(i interface{}) (s uint64) {
	fl, _ := i.(float64)
	s = uint64(fl)
	return
}

func setMillis(cf *[]Callflow) {
	for i, _ := range *cf {
		(*cf)[i].CreatedTime = (*cf)[i].CreatedTime / 1000
		(*cf)[i].ProfileCreatedTime = (*cf)[i].ProfileCreatedTime / 1000
		(*cf)[i].ProgressTime = (*cf)[i].ProgressTime / 1000
		(*cf)[i].ProgressMediaTime = (*cf)[i].ProgressMediaTime / 1000
		(*cf)[i].AnsweredTime = (*cf)[i].AnsweredTime / 1000
		(*cf)[i].BridgedTime = (*cf)[i].BridgedTime / 1000
		(*cf)[i].LastHoldTime = (*cf)[i].LastHoldTime / 1000
		(*cf)[i].HoldAccumTime = (*cf)[i].HoldAccumTime / 1000
		(*cf)[i].HangupTime = (*cf)[i].HangupTime / 1000
		(*cf)[i].ResurrectTime = (*cf)[i].ResurrectTime / 1000
		(*cf)[i].TransferTime = (*cf)[i].TransferTime / 1000
	}

}

func getQueueName(variables map[string]interface{}) (queue_name string) {
	if q, ok := getStringP(variables["cc_queue"]); ok {
		s := strings.Split(q, "@")
		if len(s) > 0 {
			queue_name = s[0]
		}
	} else if q, ok := getStringP(variables["dlr_queue"]); ok {
		s := strings.Split(q, "@")
		if len(s) > 0 {
			queue_name = s[0]
		}
	}
	return
}

func getDomainName(variables map[string]interface{}) (domain_name string) {
	if d, ok := getStringP(variables["domain_name"]); ok {
		domain_name = d
	} else if p, ok := getStringP(variables["presence_id"]); ok {
		s := strings.Split(p, "@")
		if len(s) > 0 {
			domain_name = s[len(s)-1]
		}
	}
	return
}

func getFromProfile(call, variables map[string]interface{}) (callerIdNumber, destinationNumber, callerIdName, source, networkAddr string) {
	if c, ok := call["callflow"].([]interface{}); ok && len(c) > 0 {
		callflow, ok := c[0].(map[string]interface{})["caller_profile"].(map[string]interface{})
		if ok {
			callerIdNumber, _ = getStringP(callflow["caller_id_number"])
			callerIdName, _ = getStringP(callflow["caller_id_name"])
			destinationNumber, _ = getStringP(callflow["destination_number"])
			source, _ = getStringP(callflow["source"])
			networkAddr, _ = getStringP(callflow["network_addr"])
		} else {
			destinationNumber, _ = getStringP(variables["destination_number"])
		}
	}
	return
}

func getFromStats(call map[string]interface{}) (qualityPercentageAudio, qualityPercentageVideo uint32) {
	if c, ok := call["callStats"].(map[string]interface{}); ok {
		if audio, ok := c["audio"].(map[string]interface{}); ok {
			if inbound, ok := audio["inbound"].(map[string]interface{}); ok {
				qualityPercentageAudio = getUint(inbound["quality_percentage"])
			}
		} else if video, ok := c["video"].(map[string]interface{}); ok {
			if inbound, ok := video["inbound"].(map[string]interface{}); ok {
				qualityPercentageVideo = getUint(inbound["quality_percentage"])
			}
		}
	}
	return
}

func getFromTimes(call map[string]interface{}) (createdTime /*, progressTime, answeredTime, bridgedTime, hangupTime, transferTime*/ uint64, talksec uint32) {
	if c, ok := call["callflow"].([]interface{}); ok && len(c) > 0 {
		times, ok := c[0].(map[string]interface{})["times"].(map[string]interface{})
		if ok {
			createdTime = getUintFromFloat64(times["created_time"]) / 1000 //sqlStr[0 : len(sqlStr)-3]
			var bridgedTime, hangupTime = getUintFromFloat64(times["bridged_time"]) / 1000000, getUintFromFloat64(times["hangup_time"]) / 1000000
			if bridgedTime > 0 && hangupTime > 0 {
				talksec = uint32(hangupTime - bridgedTime)
			}
			// progressTime = getUintFromFloat64(times["progress_time"]) / 1000
			// answeredTime = getUintFromFloat64(times["answered_time"]) / 1000
			// transferTime = getUintFromFloat64(times["transfer_time"]) / 1000
		}
	}
	return
}

func getExtension(variables map[string]interface{}) (extension string) {
	if a, ok := getStringP(variables["cc_agent"]); ok {
		s := strings.Split(a, "@")
		if len(s) > 0 {
			extension = s[0]
		}
	} else if u, ok := getStringP(variables["presence_id"]); ok {
		s := strings.Split(u, "@")
		if len(s) > 0 {
			extension = s[0]
		}
	} else if u, ok := getStringP(variables["dialer_user"]); ok {
		s := strings.Split(u, "@")
		if len(s) > 0 {
			extension = s[0]
		}
	} else if u := getUint(variables["bridge_epoch"]); u != 0 {
		if h, ok := getStringP(variables["last_sent_callee_id_number"]); ok {
			s := strings.Split(h, "@")
			if len(s) > 0 {
				extension = s[0]
			}
		}
	}
	return
}

func getHangupDisposition(variables map[string]interface{}) (hangup_disposition string) {
	if s, ok := getStringP(variables["hangup_disposition"]); ok {
		hangup_disposition = s
	} else if s, ok := getStringP(variables["sip_hangup_disposition"]); ok {
		hangup_disposition = s
	} else if s, ok := getStringP(variables["verto_hangup_disposition"]); ok {
		hangup_disposition = s
	}
	return
}

func getQueueHangup(variables, call map[string]interface{}) (queue_hangup uint64) {
	if _, ok := getStringP(variables["cc_queue"]); ok {
		if c, ok := getStringP(variables["cc_queue_canceled_epoch"]); ok && len(c) > 3 {
			queue_hangup, _ = strconv.ParseUint(c, 10, 64)
			queue_hangup = queue_hangup * 1000
		} else if t, ok := getStringP(variables["cc_queue_terminated_epoch"]); ok && len(c) > 3 {
			queue_hangup, _ = strconv.ParseUint(t, 10, 64)
			queue_hangup = queue_hangup * 1000
		} else if c, ok := call["callflow"].([]interface{}); ok && len(c) > 0 {
			times, ok := c[0].(map[string]interface{})["times"].(map[string]interface{})
			if ok {
				queue_hangup = getUintFromFloat64(times["hangup_time"]) / 1000
			}
		}
	}
	return
}

func getQueueAnswered(variables map[string]interface{}) (queue_answered_epoch uint64) {
	if c, ok := getStringP(variables["cc_queue_answered_epoch"]); ok && len(c) > 3 {
		queue_answered_epoch, _ = strconv.ParseUint(c, 10, 64)
	}
	return
}

func getQueueJoined(variables map[string]interface{}) (queue_joined_epoch uint64) {
	if c, ok := getStringP(variables["cc_queue_joined_epoch"]); ok && len(c) > 3 {
		queue_joined_epoch, _ = strconv.ParseUint(c, 10, 64)
	}
	return
}

func getQueueWaiting(variables map[string]interface{}) (queue_waiting uint32) {
	var first, second uint32
	if a, ok := getStringP(variables["cc_queue_answered_epoch"]); ok {
		first64, _ := strconv.ParseUint(a, 10, 32)
		first = uint32(first64)
	} else if c, ok := getStringP(variables["cc_queue_canceled_epoch"]); ok {
		first64, _ := strconv.ParseUint(c, 10, 32)
		first = uint32(first64)
	} else if t, ok := getStringP(variables["cc_queue_terminated_epoch"]); ok {
		first64, _ := strconv.ParseUint(t, 10, 32)
		first = uint32(first64)
	}
	if sec, ok := getStringP(variables["cc_queue_joined_epoch"]); ok {
		second64, _ := strconv.ParseUint(sec, 10, 32)
		second = uint32(second64)
	}
	if first > second {
		queue_waiting = first - second
	}
	return
}

func getQueueCallDuration(variables map[string]interface{}) (queue_call_duration uint32) {
	var first, second uint32
	if c, ok := getStringP(variables["cc_queue_canceled_epoch"]); ok {
		first64, _ := strconv.ParseUint(c, 10, 32)
		first = uint32(first64)
	} else if t, ok := getStringP(variables["cc_queue_terminated_epoch"]); ok {
		first64, _ := strconv.ParseUint(t, 10, 32)
		first = uint32(first64)
	}
	if sec, ok := getStringP(variables["cc_queue_joined_epoch"]); ok {
		second64, _ := strconv.ParseUint(sec, 10, 32)
		second = uint32(second64)
	}
	if first > second {
		queue_call_duration = first - second
	}
	return
}

func getQueueAnswerDelay(variables map[string]interface{}) (queue_answer_delay uint32) {
	if a, ok := getStringP(variables["cc_queue_answered_epoch"]); ok {
		if b, ok := getStringP(variables["cc_queue_joined_epoch"]); ok && a > b {
			a64, _ := strconv.ParseUint(a, 10, 32)
			b64, _ := strconv.ParseUint(b, 10, 32)
			queue_answer_delay = uint32(a64 - b64)
		}
	}
	return
}

func GenerateUuid() (uuid string) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return
	}
	uuid = strings.ToLower(fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]))
	return
}

func getParentUuid(call interface{}) string {
	var (
		s  string
		ok bool
	)
	if s, ok = call.(map[string]interface{})["variables"].(map[string]interface{})["ent_originate_aleg_uuid"].(string); !ok {
		if s, ok = call.(map[string]interface{})["variables"].(map[string]interface{})["originating_leg_uuid"].(string); !ok {
			if s, ok = call.(map[string]interface{})["variables"].(map[string]interface{})["cc_member_session_uuid"].(string); !ok {
				if s, ok = call.(map[string]interface{})["variables"].(map[string]interface{})["campon_uuid"].(string); !ok {
					if callflow, ok := call.(map[string]interface{})["callflow"].([]interface{}); ok && len(callflow) > 0 {
						if caller_profile, ok := callflow[0].(map[string]interface{})["caller_profile"].(map[string]interface{}); ok {
							if originator, ok := caller_profile["originator"].(map[string]interface{}); ok {
								if arr, ok := originator["originator_caller_profiles"].([]interface{}); ok && len(arr) > 0 {
									s, _ = arr[0].(map[string]interface{})["uuid"].(string)
								}
							}
						}
					}
				}
			}
		}
	}
	return s
}
