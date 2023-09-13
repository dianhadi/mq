package http

import (
	"net/http"
)

func (h Handler) KafkaPublish(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	w.Header().Set("Content-Type", "application/json")

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Get values from the form data
	topic := r.FormValue("topic")
	message := r.FormValue("message")

	err = h.producer.SendMessage(topic, message)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}
