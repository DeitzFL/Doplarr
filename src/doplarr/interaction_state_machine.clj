(ns doplarr.interaction-state-machine
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
   [com.rpl.specter :as s]
   [discljord.messaging :as m]
   [doplarr.discord :as discord]
   [doplarr.state :as state]
   [doplarr.utils :as utils :refer [log-on-error]]
   [fmnoise.flow :refer [else then]]
   [taoensso.timbre :refer [fatal info]]))

(def channel-timeout 600000)

(defn start-interaction! [interaction]
  (a/go
    (let [uuid (str (java.util.UUID/randomUUID))
          id (:id interaction)
          token (:token interaction)
          user-id (:user-id interaction)
          dm? (:dm? interaction)
          payload-opts (:options (:payload interaction))
          media-type (first (keys payload-opts))
          query (s/select-one [media-type :query] payload-opts)
          {:keys [messaging bot-id]} @state/discord]
      ; Ack immediately with an ephemeral "check your DMs" message
      (->> @(m/create-interaction-response! messaging id token 4 :data {:flags 64 :content (if dm? "Searching…" "Searching… check your DMs!")})
           (else #(fatal % "Error in interaction ack")))
      ; Search for results
      (info "Performing search for" (name media-type) query)
      (let [results (->> (log-on-error
                          (a/<! ((utils/media-fn media-type "search") query media-type))
                          "Exception from search")
                         (then #(->> (take (:max-results @state/config discord/MAX-OPTIONS) %)
                                     (into []))))]
        ; Open DM channel and send search results there
        (let [dm-ch  @(m/create-dm! messaging user-id)
              dm-msg @(m/create-message! messaging (:id dm-ch) (discord/search-response results uuid))]
          ; Setup ttl cache entry with DM coordinates
          (swap! state/cache assoc uuid {:results results
                                         :media-type media-type
                                         :token token
                                         :dm-channel-id (:id dm-ch)
                                         :dm-message-id (:id dm-msg)
                                         :last-modified (System/currentTimeMillis)}))))))

(defn query-for-option-or-request [pending-opts uuid]
  (a/go
    (let [{:keys [messaging]} @state/discord
          {:keys [media-type payload dm-channel-id dm-message-id]} (get @state/cache uuid)]
      (if (empty? pending-opts)
        (let [embed (log-on-error
                     (a/<! ((utils/media-fn media-type "request-embed") payload media-type))
                     "Exception from request-embed")]
          (swap! state/cache assoc-in [uuid :embed] embed)
          (->> @(m/edit-message! messaging dm-channel-id dm-message-id (discord/request embed uuid))
               (else #(fatal % "Error in sending request embed"))))
        (let [[op options] (first pending-opts)]
          (->> @(m/edit-message! messaging dm-channel-id dm-message-id (discord/option-dropdown op options uuid 0))
               (else #(fatal % "Error in creating option dropdown"))))))))

(defmulti process-event! (fn [event _ _ _] event))

(defmethod process-event! "result-select" [_ interaction uuid _]
  (a/go
    (let [{:keys [results media-type]} (get @state/cache uuid)
          result (nth results (discord/dropdown-result interaction))
          add-opts (log-on-error
                    (a/<! ((utils/media-fn media-type "additional-options") result media-type))
                    "Exception thrown from additional-options")
          pending-opts (->> add-opts
                            (filter #(seq? (second %)))
                            (into {}))
          ready-opts (apply (partial dissoc add-opts) (keys pending-opts))]
                                        ; Start setting up the payload
      (swap! state/cache assoc-in [uuid :payload] result)
      (swap! state/cache assoc-in [uuid :pending-opts] pending-opts)
                                        ; Merge in the opts that are already satisfied
      (swap! state/cache update-in [uuid :payload] merge ready-opts)
      (query-for-option-or-request pending-opts uuid))))

(defmethod process-event! "option-page" [_ _ uuid option]
  (let [{:keys [messaging]} @state/discord
        {:keys [pending-opts dm-channel-id dm-message-id]} (get @state/cache uuid)
        [opt page] (str/split option #"-")
        op (keyword opt)
        page (Long/parseLong page)
        options (op pending-opts)]
    (->> @(m/edit-message! messaging dm-channel-id dm-message-id (discord/option-dropdown op options uuid page))
         (else #(fatal % "Error in updating option dropdown")))))

(defmethod process-event! "option-select" [_ interaction uuid option]
  (let [selection (discord/dropdown-result interaction)
        cache-val (swap! state/cache update-in [uuid :pending-opts] #(dissoc % (keyword option)))]
    (swap! state/cache assoc-in [uuid :payload (keyword option)] selection)
    (query-for-option-or-request (get-in cache-val [uuid :pending-opts]) uuid)))

(defmethod process-event! "request" [_ interaction uuid format]
  (let [{:keys [messaging]} @state/discord
        {:keys [payload media-type embed dm-channel-id dm-message-id]} (get @state/cache uuid)
        {:keys [user-id]} interaction]
    (letfn [(msg-resp [msg] (->> @(m/edit-message! messaging dm-channel-id dm-message-id (discord/content-response msg))
                                 (else #(fatal % "Error in message response"))))]
      (->> (log-on-error
            (a/<!! ((utils/media-fn media-type "request")
                    (assoc payload :format (keyword format) :discord-id user-id)
                    media-type))
            "Exception from request")
           (then (fn [status]
                   (case status
                     :unauthorized (msg-resp "You are unauthorized to perform this request in the configured backend")
                     :pending (msg-resp "This has already been requested and the request is pending")
                     :processing (msg-resp "This is currently processing and should be available soon!")
                     :available (msg-resp "This selection is already available!")
                     (do
                       (info "Performing request for " payload)
                       (->> @(m/edit-message! messaging dm-channel-id dm-message-id
                                              (discord/request-performed-embed embed user-id))
                            (else #(fatal % "Error in request confirmation")))
                       (swap! state/pending-requests assoc
                              [user-id (:title embed)]
                              {:dm-channel-id dm-channel-id
                               :dm-message-id dm-message-id
                               :embed         embed})))))
           (else (fn [e]
                   (let [{:keys [status body] :as data} (ex-data e)]
                     (if (= status 403)
                       (msg-resp (body "message"))
                       (->> (msg-resp "Unspecified error on request, check logs")
                            (then #(fatal "Non 403 error on request" % data)))))))))))

(defn continue-interaction! [interaction]
  (let [[event uuid option] (str/split (s/select-one [:payload :component-id] interaction) #":")
        now (System/currentTimeMillis)
        {:keys [token id]} interaction
        {:keys [messaging bot-id]} @state/discord]
    ; Send the ack
    (->> @(m/create-interaction-response! messaging id token 6)
         (else #(fatal % "Error sending response ack")))
    ; Check last modified
    (if-let [{:keys [last-modified dm-channel-id dm-message-id]} (get @state/cache uuid)]
      (if (> (- now last-modified) channel-timeout)
        ; Timed out — edit the DM message directly
        (->> @(m/edit-message! messaging dm-channel-id dm-message-id (discord/content-response "Request timed out, please try again"))
             (else #(fatal % "Error in sending timeout response")))
        ; Move through the state machine
        (do
          (swap! state/cache assoc-in [uuid :last-modified] now)
          (process-event! event interaction uuid option)))
      ; Cache miss — fall back to the component interaction's own token
      (->> @(m/edit-original-interaction-response! messaging bot-id token (discord/content-response "Request timed out, please try again"))
           (else #(fatal % "Error in sending timeout response"))))))
