(ns doplarr.core
  (:require
   [clojure.core.async :as a]
   [config.core :refer [load-env]]
   [discljord.connections :as c]
   [discljord.events :as e]
   [discljord.messaging :as m]
   [doplarr.backends.overseerr.impl :as overseerr]
   [doplarr.config :as config]
   [doplarr.discord :as discord]
   [doplarr.interaction-state-machine :as ism]
   [doplarr.state :as state]
   [doplarr.webhook :as webhook]
   [taoensso.timbre :refer [debug fatal info] :as timbre]
   [taoensso.timbre.tools.logging :as tlog])
  (:gen-class))

; Pipe tools.logging to timbre
(tlog/use-timbre)

; Multimethod for handling incoming Discord events
(defmulti handle-event!
  (fn [event-type _]
    event-type))

; A new interaction was received (slash command or component)
(defmethod handle-event! :interaction-create
  [_ data]
  (debug "Received interaction")
  (let [interaction (discord/interaction-data data)]
    (case (:type interaction)
      ; Slash commands start our request sequence
      :application-command (ism/start-interaction! interaction)
      ; Message components continue the request until they are complete or failed
      :message-component (ism/continue-interaction! interaction))))

; Once we receive a ready event, grab our bot-id
(defmethod handle-event! :ready
  [_ {{id :id} :user}]
  (info "Discord connection successful")
  (swap! state/discord assoc :bot-id id)
  (let [media-types (config/available-media @state/config)
        messaging (:messaging @state/discord)]
    (discord/register-commands media-types id messaging)))

(defmethod handle-event! :guild-create
  [_ _]
  (info "Connected to guild"))

(defmethod handle-event! :message-reaction-add
  [_ data]
  (let [{:keys [user-id emoji]} data
        message-id (str (:message-id data))
        {:keys [bot-id messaging]} @state/discord]
    (when (and (not= user-id bot-id)
               (= (:name emoji) "🗑️")
               (contains? @state/reaction-watch message-id))
      (let [{:keys [seerr-id discord-id dm-channel-id title]} (get @state/reaction-watch message-id)]
        (when (= user-id discord-id)
          (info "Deleting media" title "for user" user-id)
          (swap! state/reaction-watch dissoc message-id)
          (a/go
            (a/<! (overseerr/delete-media-files! seerr-id))
            (a/<! (overseerr/delete-media! seerr-id))
            (a/<! (m/edit-message! messaging dm-channel-id message-id
                                   (discord/content-response "Deleted from your library.")))))))))

(defmethod handle-event! :default
  [event-type data]
  (debug "Got unhandled event" event-type data))

(defn start-bot! []
  (let [event-ch (a/chan 100)
        token (:discord/token @state/config)
        connection-ch (c/connect-bot! token event-ch :intents #{:guilds :direct-message-reactions})
        messaging-ch (m/start-connection! token)
        init-state {:connection connection-ch
                    :event event-ch
                    :messaging messaging-ch}]
    (reset! state/discord init-state)
    (try (e/message-pump! event-ch handle-event!)
         (catch Exception e (fatal e "Exception thrown from event handler"))
         (finally
           (m/stop-connection! messaging-ch)
           (a/close! event-ch)))))

(defn setup-config! []
  (reset! state/config (config/valid-config (load-env)))
  (timbre/merge-config! {:min-level [[#{"*"} (:log-level @state/config :info)]]
                         :output-fn (partial timbre/default-output-fn {:stacktrace-fonts {}})}))

(defn startup! []
  (setup-config!)
  (webhook/start-server!)
  (start-bot!))

; Program Entry Point
(defn -main
  [& _]
  (startup!)
  (shutdown-agents))
