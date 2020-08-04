package messaging

import (
	"encoding/hex"
	"encoding/json"
	"github.com/satori/go.uuid"
	"log"
)

type Message struct {
	Message_body interface{} `json:"oslo.message"`
	Version      string      `json:"oslo.version"`
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

type OsloReply struct {
	Result    string      `json:"result"`
	Failure   interface{} `json:"failure"`
	Ending    bool        `json:"ending"`
	Msg_id    string      `json:"_msg_id"`
	Unique_id string      `json:"_unique_id"`
}

func (msg *OsloMessage) Reply(reply_msg interface{}, conn *AMQPConnection) {
	unique_id := generate_uuid()
	reply := OsloReply{Result: reply_msg.(string),
		//Failure: nil,
		Ending:    true,
		Msg_id:    msg.Msg_id,
		Unique_id: unique_id}
	ReplyMsg, err := json.Marshal(reply)
	if err != nil {
		log.Println("序列化失败:", msg)
	}

	conn.SendReply(msg.Reply_queue, ReplyMsg)
}

func gethex(u uuid.UUID) string {
	buf := make([]byte, 32)

	hex.Encode(buf[0:], u[0:])
	//buf[8] = '-'
	//hex.Encode(buf[9:13], u[4:6])
	//buf[13] = '-'
	//hex.Encode(buf[14:18], u[6:8])
	//buf[18] = '-'
	//hex.Encode(buf[19:23], u[8:10])
	//buf[23] = '-'
	//hex.Encode(buf[24:], u[10:])

	return string(buf)
}

func generate_uuid() string {
	u, _ := uuid.NewV4()
	return gethex(u)

}
