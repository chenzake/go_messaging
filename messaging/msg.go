package messaging

type Message struct {
	Message_body string `json:"oslo.message"`
	Version      string `json:"oslo.version"`
}

type OsloMessage struct {
	Unique_id   string                  `json:"_unique_id"`
	Msg_id      string                  `json:"_msg_id"`
	Timeout     uint8                   `json:"_timeout"`
	Reply_queue string                  `json:"_reply_q"`
	Args        *map[string]interface{} `json:"args"`
	Method      string                  `json:"method"`
}

//func (Msg *Message) Serialize_msg(msg *Message) string {
//	j, err := json.Marshal(msg)
//	if err != nil {
//		fmt.Println("生成json字符串错误")
//	}
//	fmt.Println(string(j))
//	return string(j)
//}
