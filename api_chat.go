package dify

import (
	"context"
	"net/http"
)

type ChatMessageRequest struct {
	Inputs         map[string]interface{} `json:"inputs"`
	Query          string                 `json:"query"`
	ResponseMode   string                 `json:"response_mode"`
	ConversationID string                 `json:"conversation_id,omitempty"`
	User           string                 `json:"user"`
}

type ChatMessageResponse struct {
	Event          string         `json:"event"`
	ID             string         `json:"id"`
	MessageID      string         `json:"message_id"`
	TaskID         string         `json:"task_id"`
	Mode           string         `json:"mode"`
	Answer         string         `json:"answer"`
	ConversationID string         `json:"conversation_id"`
	Metadata       map[string]any `json:"metadata"`
	CreatedAt      int            `json:"created_at"`
}

/* Create chat message
 * Create a new conversation message or continue an existing dialogue.
 */
func (api *API) ChatMessages(ctx context.Context, req *ChatMessageRequest) (resp *ChatMessageResponse, err error) {
	req.ResponseMode = "blocking"

	httpReq, err := api.createBaseRequest(ctx, http.MethodPost, "/v1/chat-messages", req)
	if err != nil {
		return
	}
	err = api.c.sendJSONRequest(httpReq, &resp)
	return
}
