(ns doplarr.state
  (:require
   [clojure.core.cache.wrapped :as cache]))

(def cache (cache/ttl-cache-factory {} :ttl 900000))

(def discord (atom nil))

(def config (atom nil))

; Long-lived store of pending requests: {[discord-user-id title] {:dm-channel-id :dm-message-id :embed}}
; Used by the webhook handler to delete the "received" message and send a fresh "available" notification.
(def pending-requests (atom {}))

; {message-id {:seerr-id N :discord-id "..." :dm-channel-id "..." :title "..."}}
; Used by the reaction handler to map a trash-emoji reaction to a Seerr delete call.
(def reaction-watch (atom {}))
