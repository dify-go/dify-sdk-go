package dify

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	jsoniter "github.com/json-iterator/go"
)

type ChatStreamMessageData struct {
	Event          string `json:"event"`
	TaskID         string `json:"task_id"`
	Answer         string `json:"answer"`
	CreatedAt      int64  `json:"created_at"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
}

type ChatStreamMessageFileData struct {
	Event          string `json:"event"`
	ID             string `json:"id"`
	Type           string `json:"type"`
	BelongsTo      string `json:"belongs_to"`
	URL            string `json:"url"`
	CreatedAt      int64  `json:"created_at"`
	ConversationID string `json:"conversation_id"`
}

type ChatStreamMessageEndData struct {
	Event          string         `json:"event"`
	TaskID         string         `json:"task_id"`
	MessageID      string         `json:"message_id"`
	CreatedAt      int64          `json:"created_at"`
	ConversationID string         `json:"conversation_id"`
	Metadata       map[string]any `json:"metadata"`
}

type ChatStreamTTSMessageData struct {
	Event     string `json:"event"`
	TaskID    string `json:"task_id"`
	MessageID string `json:"message_id"`
	CreatedAt int64  `json:"created_at"`
	Audio     string `json:"audio"`
}

type ChatStreamTTSMessageEndData struct {
	Event     string `json:"event"`
	TaskID    string `json:"task_id"`
	MessageID string `json:"message_id"`
	CreatedAt int64  `json:"created_at"`
	Audio     string `json:"audio"`
}

type ChatStreamMessageReplaceData struct {
	Event          string `json:"event"`
	TaskID         string `json:"task_id"`
	Answer         string `json:"answer"`
	CreatedAt      int64  `json:"created_at"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
}

type ChatStreamWorkflowStartedData struct {
	Event         string `json:"event"`
	TaskID        string `json:"task_id"`
	WorkflowRunID string `json:"workflow_run_id"`
	Data          struct {
		ID             string    `json:"id"`
		WorkflowID     string    `json:"workflow_id"`
		SequenceNumber int       `json:"sequence_number"`
		CreatedAt      time.Time `json:"created_at"`
	} `json:"data"`
}

type ChatStreamWorkflowFinishedData struct {
	Event         string `json:"event"`
	TaskID        string `json:"task_id"`
	WorkflowRunID string `json:"workflow_run_id"`
	Data          struct {
		ID          string         `json:"id"`
		WorkflowID  string         `json:"workflow_id"`
		Status      string         `json:"status"`
		Outputs     map[string]any `json:"outputs"`
		Error       string         `json:"error"`
		ElapsedTime float64        `json:"elapsed_time"`
		TotalTokens int            `json:"total_tokens"`
		TotalSteps  int            `json:"total_steps"`
		CreatedAt   time.Time      `json:"created_at"`
		FinishedAt  time.Time      `json:"finished_at"`
	} `json:"data"`
}

type ChatStreamNodeStartedData struct {
	Event         string `json:"event"`
	TaskID        string `json:"task_id"`
	WorkflowRunID string `json:"workflow_run_id"`
	Data          struct {
		ID                string         `json:"id"`
		NodeID            string         `json:"node_id"`
		NodeType          string         `json:"node_type"`
		Title             string         `json:"title"`
		Index             int            `json:"index"`
		PredecessorNodeID string         `json:"predecessor_node_id"`
		Inputs            map[string]any `json:"inputs"`
		CreatedAt         time.Time      `json:"created_at"`
	} `json:"data"`
}

type ChatStreamNodeFinishedData struct {
	Event         string `json:"event"`
	TaskID        string `json:"task_id"`
	WorkflowRunID string `json:"workflow_run_id"`
	Data          struct {
		ID                string         `json:"id"`
		NodeID            string         `json:"node_id"`
		Title             string         `json:"title"`
		Index             int            `json:"index"`
		PredecessorNodeID string         `json:"predecessor_node_id"`
		Inputs            map[string]any `json:"inputs"`
		ProcessData       map[string]any `json:"process_data"`
		Status            string         `json:"status"`
		Error             string         `json:"error"`
		ElapsedTime       float64        `json:"elapsed_time"`
		CreatedAt         time.Time      `json:"created_at"`
		ExecutionMetadata struct {
			TotalTokens int     `json:"total_tokens"`
			TotalPrice  float64 `json:"total_price"`
			Currency    string  `json:"currency"`
		} `json:"execution_metadata"`
	} `json:"data"`
}

type ChatStreamErrorData struct {
	Event     string `json:"event"`
	TaskID    string `json:"task_id"`
	MessageID string `json:"message_id"`
	Status    string `json:"status"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

type ChatMessageStreamChannelResponse struct {
	Event string `json:"event"`
	Data  any    `json:"data"`
	Err   error  `json:"-"`
}

func (api *API) ChatMessagesStreamRaw(ctx context.Context, req *ChatMessageRequest) (*http.Response, error) {
	req.ResponseMode = "streaming"

	httpReq, err := api.createBaseRequest(ctx, http.MethodPost, "/v1/chat-messages", req)
	if err != nil {
		return nil, err
	}
	return api.c.sendRequest(httpReq)
}

func (api *API) ChatMessagesStream(ctx context.Context, req *ChatMessageRequest) (chan ChatMessageStreamChannelResponse, error) {
	httpResp, err := api.ChatMessagesStreamRaw(ctx, req)
	if err != nil {
		return nil, err
	}

	streamChannel := make(chan ChatMessageStreamChannelResponse)
	go api.chatMessagesStreamHandle(ctx, httpResp, streamChannel)
	return streamChannel, nil
}

func (api *API) chatMessagesStreamHandle(ctx context.Context, resp *http.Response, streamChannel chan ChatMessageStreamChannelResponse) {
	defer resp.Body.Close()
	defer close(streamChannel)

	reader := bufio.NewReader(resp.Body)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					return
				}
				streamChannel <- ChatMessageStreamChannelResponse{
					Err: fmt.Errorf("error reading line: %w", err),
				}
				return
			}

			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			line = bytes.TrimPrefix(line, []byte("data:"))
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}

			event := jsoniter.Get(line, "event").ToString()

			resp := ChatMessageStreamChannelResponse{
				Event: event,
			}

			switch event {
			case "message":
				var data ChatStreamMessageData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}

			case "message_file":
				var data ChatStreamMessageFileData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}
			case "message_end":
				var data ChatStreamMessageEndData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}
			case "tts_message":
				var data ChatStreamTTSMessageData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}
			case "tts_message_end":
				var data ChatStreamTTSMessageEndData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}
			case "workflow_started":
				var data ChatStreamWorkflowStartedData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}
			case "workflow_finished":
				var data ChatStreamWorkflowFinishedData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}
			case "node_started":
				var data ChatStreamNodeStartedData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}
			case "node_finished":
				var data ChatStreamNodeFinishedData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}
			case "error":
				var data ChatStreamErrorData
				if err = json.Unmarshal(line, &data); err != nil {
					resp.Err = err
					resp.Event = "error"
				} else {
					resp.Data = &data
				}
			}

			if resp.Err != nil {
				streamChannel <- resp
				return
			}

			streamChannel <- resp
		}
	}
}
