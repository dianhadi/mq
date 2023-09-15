package http

import (
	"fmt"
	"net/http"
	"strconv"
)

func (h Handler) Publish(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	w.Header().Set("Content-Type", "application/json")

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Get values from the form data
	topic := r.FormValue("topic")
	message := r.FormValue("message")
	direct, _ := strconv.ParseBool(r.FormValue("direct"))

	if direct {
		err = h.producer.SendDirectMessage(topic, message)
	} else {
		err = h.producer.SendMessage(topic, message)
	}
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}
