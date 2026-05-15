(ns doplarr.webhook
  (:require
   [cheshire.core :as json]
   [clojure.core.async :as a]
   [clojure.string :as str]
   [discljord.messaging :as m]
   [doplarr.backends.overseerr.impl :as overseerr]
   [doplarr.discord :as discord]
   [doplarr.state :as state]
   [org.httpkit.server :as server]
   [taoensso.timbre :refer [error info warn]]))

(defn find-pending [discord-id subject]
  (->> @state/pending-requests
       (filter (fn [[[uid title] _]]
                 (and (= uid discord-id)
                      title
                      (str/includes? subject title))))
       first))

(defn notify-available! [discord-id subject tmdb-id media-type-str]
  (when-let [{:keys [messaging]} @state/discord]
    (let [dm-ch      @(m/create-dm! messaging discord-id)
          channel-id (:id dm-ch)
          pending    (find-pending discord-id subject)]
      ; Delete the "received" message so only one notification exists per request
      (when pending
        (let [[key {:keys [dm-channel-id dm-message-id]}] pending]
          (swap! state/pending-requests dissoc key)
          @(m/delete-message! messaging dm-channel-id dm-message-id)))
      ; Send fresh message — triggers a real Discord notification badge
      ; Includes full TMDB embed (poster, overview, thumbnail) if we have the stored data;
      ; falls back to plain text if the bot restarted and lost the in-memory store.
      (let [embed (some-> pending second :embed)
            msg   @(m/create-message! messaging channel-id
                                      (if embed
                                        {:content "Your request is now available! React with 🗑️ to delete from your library."
                                         :embeds  [(discord/request-embed embed)]}
                                        {:content (str "Your request is now available: **" subject "**\nReact with 🗑️ to delete from your library.")}))]
        ; In background: look up Seerr internal ID, add reaction, register for reaction-watch
        (when (and msg tmdb-id)
          (a/go
            (let [msg-id   (str (:id msg))
                  seerr-id (a/<! (overseerr/seerr-media-id tmdb-id media-type-str))]
              (a/<! (m/create-reaction! messaging channel-id msg-id "🗑️"))
              (when seerr-id
                (swap! state/reaction-watch assoc msg-id
                       {:seerr-id      seerr-id
                        :discord-id    discord-id
                        :dm-channel-id channel-id
                        :title         subject})))))))))

(defn handle-webhook [req]
  (try
    (let [body  (json/parse-string (slurp (:body req)) true)
          ntype (:notification_type body)]
      (if (= ntype "MEDIA_AVAILABLE")
        (let [discord-id     (get-in body [:request :requestedBy_settings_discordId])
              subject        (:subject body)
              tmdb-id        (get-in body [:media :tmdbId])
              media-type-str (get-in body [:media :media_type])]
          (if discord-id
            (do (info "Notifying Discord user" discord-id "— media available:" subject)
                (notify-available! discord-id subject tmdb-id media-type-str)
                {:status 200 :body "ok"})
            (do (warn "MEDIA_AVAILABLE with no Discord ID — user may not have Discord linked in Overseerr")
                {:status 200 :body "no discord id"})))
        (do (info "Ignoring webhook event:" ntype)
            {:status 200 :body "ignored"})))
    (catch Exception e
      (error e "Error processing webhook")
      {:status 500 :body "error"})))

(defn handler [req]
  (if (= (:uri req) "/webhook")
    (handle-webhook req)
    {:status 404 :body "not found"}))

(defn start-server! []
  (let [port 8081]
    (info "Starting webhook server on port" port)
    (server/run-server handler {:port port})))
