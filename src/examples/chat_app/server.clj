(ns examples.chat-app.server
  "Real-time chat application demonstrating Reactor's capabilities"
  (:require [reactor.core :as r]
            [reactor.frame :as rf]
            [reactor.sse :as sse]
            [org.httpkit.server :as http]
            [compojure.core :refer [defroutes GET POST routes]]
            [compojure.route :as route]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.content-type :refer [wrap-content-type]]
            [ring.middleware.not-modified :refer [wrap-not-modified]]
            [clojure.string :as str]))

;; Initialize chat app with time-travel enabled
(def chat-app (rf/create-frame-app
               {:messages []
                :users {}
                :typing {}
                :rooms {:general {:name "General"
                                  :description "General discussion"
                                  :messages []
                                  :users #{}}
                        :random {:name "Random"
                                 :description "Off-topic chat"
                                 :messages []
                                 :users #{}}}}
               {:history true
                :max-history 100}))

;; Subscriptions
(rf/reg-sub :messages
  (fn [db [room-id]]
    (get-in db [:rooms room-id :messages] [])))

(rf/reg-sub :all-messages
  (fn [db _]
    (:messages db)))

(rf/reg-sub :online-users
  (fn [db _]
    (filter #(:online (val %)) (:users db))))

(rf/reg-sub :room-users
  (fn [db [room-id]]
    (let [user-ids (get-in db [:rooms room-id :users] #{})]
      (select-keys (:users db) user-ids))))

(rf/reg-sub :typing-users
  (fn [db [room-id]]
    (get-in db [:typing room-id] {})))

(rf/reg-sub :rooms
  (fn [db _]
    (:rooms db)))

(rf/reg-sub :room-info
  (fn [db [room-id]]
    (get-in db [:rooms room-id])))

;; Events
(rf/reg-event-fx :user-join
  (fn [{:keys [db]} [user-id username]]
    {:db (-> db
             (assoc-in [:users user-id] {:id user-id
                                          :username username
                                          :online true
                                          :joined-at (System/currentTimeMillis)})
             (update :messages conj {:type :system
                                      :text (str username " joined the chat")
                                      :timestamp (System/currentTimeMillis)}))
     :dispatch [:broadcast-user-list]}))

(rf/reg-event-fx :user-leave
  (fn [{:keys [db]} [user-id]]
    (let [username (get-in db [:users user-id :username])]
      {:db (-> db
               (update-in [:users user-id] assoc :online false)
               (update :messages conj {:type :system
                                        :text (str username " left the chat")
                                        :timestamp (System/currentTimeMillis)}))
       :dispatch [:broadcast-user-list]})))

(rf/reg-event-fx :send-message
  (fn [{:keys [db]} [user-id room-id text]]
    (let [username (get-in db [:users user-id :username])
          message {:id (str (java.util.UUID/randomUUID))
                   :user-id user-id
                   :username username
                   :text text
                   :room-id room-id
                   :timestamp (System/currentTimeMillis)
                   :type :user}]
      {:db (-> db
               (update :messages conj message)
               (update-in [:rooms room-id :messages] conj message))
       :dispatch-n [[:stop-typing user-id room-id]
                    [:broadcast-message message]]})))

(rf/reg-event-db :start-typing
  (fn [db [user-id room-id]]
    (assoc-in db [:typing room-id user-id] 
              {:username (get-in db [:users user-id :username])
               :timestamp (System/currentTimeMillis)})))

(rf/reg-event-db :stop-typing
  (fn [db [user-id room-id]]
    (update-in db [:typing room-id] dissoc user-id)))

(rf/reg-event-db :join-room
  (fn [db [user-id room-id]]
    (-> db
        (update-in [:rooms room-id :users] conj user-id)
        (update-in [:rooms room-id :messages] conj
                   {:type :system
                    :text (str (get-in db [:users user-id :username]) " joined the room")
                    :timestamp (System/currentTimeMillis)}))))

(rf/reg-event-db :leave-room
  (fn [db [user-id room-id]]
    (-> db
        (update-in [:rooms room-id :users] disj user-id)
        (update-in [:rooms room-id :messages] conj
                   {:type :system
                    :text (str (get-in db [:users user-id :username]) " left the room")
                    :timestamp (System/currentTimeMillis)}))))

(rf/reg-event-fx :create-room
  (fn [{:keys [db]} [room-id room-name description]]
    {:db (assoc-in db [:rooms room-id] {:name room-name
                                          :description description
                                          :messages []
                                          :users #{}
                                          :created-at (System/currentTimeMillis)})
     :dispatch [:broadcast-rooms]}))

;; Broadcast events (for SSE updates)
(rf/reg-event-fx :broadcast-message
  (fn [{:keys [db]} [message]]
    (println "Broadcasting message:" (:text message))
    {:db db}))

(rf/reg-event-fx :broadcast-user-list
  (fn [{:keys [db]} _]
    (println "Broadcasting user list")
    {:db db}))

(rf/reg-event-fx :broadcast-rooms
  (fn [{:keys [db]} _]
    (println "Broadcasting room list")
    {:db db}))

;; Rules for auto-moderation and features
(r/def-rule (:app-db chat-app) :auto-clear-typing [:typing]
            (fn [_ typing-map]
              ;; Clear typing indicators older than 3 seconds
              (let [now (System/currentTimeMillis)]
                (doseq [[room-id users] typing-map]
                  (doseq [[user-id data] users]
                    (when (> (- now (:timestamp data)) 3000)
                      ((:dispatch chat-app) [:stop-typing user-id room-id])))))))

(r/def-rule (:app-db chat-app) :profanity-filter [:messages]
            (fn [_ messages]
              (when-let [last-msg (last messages)]
                (when (and (= (:type last-msg) :user)
                           (re-find #"(?i)(badword1|badword2)" (:text last-msg)))
                  (println "⚠️ Profanity detected from user:" (:username last-msg))))))

(r/def-rule (:app-db chat-app) :activity-logger [:messages]
            (fn [_ messages]
              (when (= 0 (mod (count messages) 10))
                (println "📊 Total messages:" (count messages)))))

;; HTML page
(def chat-html
  "<!DOCTYPE html>
<html>
<head>
    <title>Reactor Chat</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 0; }
        #app { display: flex; height: 100vh; }
        .sidebar { width: 250px; background: #2c3e50; color: white; padding: 20px; }
        .chat-area { flex: 1; display: flex; flex-direction: column; }
        .messages { flex: 1; padding: 20px; overflow-y: auto; background: #ecf0f1; }
        .message { margin: 10px 0; padding: 10px; background: white; border-radius: 5px; }
        .message.system { background: #95a5a6; color: white; font-style: italic; }
        .input-area { padding: 20px; background: white; border-top: 1px solid #bdc3c7; }
        .input-area input { width: 100%; padding: 10px; font-size: 16px; border: 1px solid #bdc3c7; border-radius: 5px; }
        .user-list { margin-top: 20px; }
        .user { padding: 5px 0; }
        .room-list { margin-top: 30px; }
        .room { padding: 8px; margin: 5px 0; background: #34495e; border-radius: 5px; cursor: pointer; }
        .room.active { background: #3498db; }
        h1 { margin: 0 0 20px 0; }
        h3 { margin: 20px 0 10px 0; }
    </style>
</head>
<body>
    <div id=\"app\">
        <div class=\"sidebar\">
            <h1>💬 Reactor Chat</h1>
            <div class=\"user-info\">
                <input type=\"text\" id=\"username\" placeholder=\"Enter username...\" />
                <button onclick=\"joinChat()\">Join Chat</button>
            </div>
            <div class=\"room-list\">
                <h3>Rooms</h3>
                <div class=\"room active\" data-room=\"general\">📢 General</div>
                <div class=\"room\" data-room=\"random\">🎲 Random</div>
            </div>
            <div class=\"user-list\">
                <h3>Online Users</h3>
                <div id=\"users\"></div>
            </div>
        </div>
        <div class=\"chat-area\">
            <div class=\"messages\" id=\"messages\">
                <div class=\"message system\">Welcome to Reactor Chat! Enter a username to start.</div>
            </div>
            <div class=\"input-area\">
                <input type=\"text\" id=\"message-input\" placeholder=\"Type a message...\" disabled onkeypress=\"handleKeyPress(event)\" />
            </div>
        </div>
    </div>
    
    <script>
        let userId = null;
        let username = null;
        let currentRoom = 'general';
        let eventSource = null;
        
        function escapeEDN(str) {
            return str.replace(/\\/g, '\\\\\\\\').replace(/\"/g, '\\\\\"');
        }
        
        function joinChat() {
            const input = document.getElementById('username');
            username = input.value.trim();
            if (!username) return;
            
            userId = 'user-' + Date.now();
            
            // Connect SSE
            eventSource = new EventSource('/subscribe?format=json');
            eventSource.onmessage = function(event) {
                console.log('SSE received:', event.data);
                try {
                    const state = JSON.parse(event.data);
                    updateUI(state);
                } catch(e) {
                    console.error('Failed to parse SSE data:', e);
                }
            };
            
            // Join the chat
            const joinBody = '{:user-id \"' + userId + '\" :username \"' + escapeEDN(username) + '\"}';
            console.log('Joining with:', joinBody);
            fetch('/api/join', {
                method: 'POST',
                headers: {'Content-Type': 'application/edn'},
                body: joinBody
            }).then(r => console.log('Join response:', r.status));
            
            // Join general room
            const roomBody = '{:user-id \"' + userId + '\" :room-id :' + currentRoom + '}';
            console.log('Joining room with:', roomBody);
            fetch('/api/room/join', {
                method: 'POST',
                headers: {'Content-Type': 'application/edn'},
                body: roomBody
            }).then(r => console.log('Room join response:', r.status));
            
            document.getElementById('message-input').disabled = false;
            input.disabled = true;
        }
        
        function sendMessage() {
            const input = document.getElementById('message-input');
            const text = input.value.trim();
            if (!text || !userId) return;
            
            const msgBody = '{:user-id \"' + userId + '\" :room-id :' + currentRoom + ' :text \"' + escapeEDN(text) + '\"}';
            console.log('Sending message:', msgBody);
            fetch('/api/message', {
                method: 'POST',
                headers: {'Content-Type': 'application/edn'},
                body: msgBody
            }).then(r => console.log('Message response:', r.status));
            
            input.value = '';
        }
        
        function handleKeyPress(event) {
            if (event.key === 'Enter') {
                sendMessage();
            }
        }
        
        function updateUI(state) {
            // Update messages
            const messagesDiv = document.getElementById('messages');
            messagesDiv.innerHTML = '';
            
            const roomMessages = state.rooms[currentRoom]?.messages || [];
            roomMessages.forEach(msg => {
                const div = document.createElement('div');
                div.className = msg.type === 'system' ? 'message system' : 'message';
                div.innerHTML = msg.type === 'system' 
                    ? msg.text 
                    : '<strong>' + (msg.username || 'Unknown') + ':</strong> ' + msg.text;
                messagesDiv.appendChild(div);
            });
            
            messagesDiv.scrollTop = messagesDiv.scrollHeight;
            
            // Update users
            const usersDiv = document.getElementById('users');
            usersDiv.innerHTML = '';
            Object.values(state.users || {}).forEach(user => {
                if (user.online) {
                    const div = document.createElement('div');
                    div.className = 'user';
                    div.textContent = '🟢 ' + user.username;
                    usersDiv.appendChild(div);
                }
            });
        }
        
        // Room switching
        document.querySelectorAll('.room').forEach(room => {
            room.addEventListener('click', function() {
                const newRoom = this.dataset.room;
                if (newRoom === currentRoom || !userId) return;
                
                // Leave current room
                fetch('/api/room/leave', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/edn'},
                    body: '{:user-id \"' + userId + '\" :room-id :' + currentRoom + '}'
                });
                
                // Join new room
                currentRoom = newRoom;
                fetch('/api/room/join', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/edn'},
                    body: '{:user-id \"' + userId + '\" :room-id :' + currentRoom + '}'
                });
                
                // Update UI
                document.querySelectorAll('.room').forEach(r => r.classList.remove('active'));
                this.classList.add('active');
            });
        });
    </script>
</body>
</html>")

;; API Routes
(defroutes chat-routes
  (GET "/" []
    {:status 200
     :headers {"Content-Type" "text/html"}
     :body chat-html})
  
  (POST "/api/join" req
    (let [{:keys [user-id username]} (read-string (slurp (:body req)))]
      ((:dispatch chat-app) [:user-join user-id username])
      {:status 200 :body "OK"}))
  
  (POST "/api/leave" req
    (let [{:keys [user-id]} (read-string (slurp (:body req)))]
      ((:dispatch chat-app) [:user-leave user-id])
      {:status 200 :body "OK"}))
  
  (POST "/api/message" req
    (let [{:keys [user-id room-id text]} (read-string (slurp (:body req)))]
      ((:dispatch chat-app) [:send-message user-id room-id text])
      {:status 200 :body "OK"}))
  
  (POST "/api/typing" req
    (let [{:keys [user-id room-id typing]} (read-string (slurp (:body req)))]
      (if typing
        ((:dispatch chat-app) [:start-typing user-id room-id])
        ((:dispatch chat-app) [:stop-typing user-id room-id]))
      {:status 200 :body "OK"}))
  
  (POST "/api/room/join" req
    (let [{:keys [user-id room-id]} (read-string (slurp (:body req)))]
      ((:dispatch chat-app) [:join-room user-id room-id])
      {:status 200 :body "OK"}))
  
  (POST "/api/room/leave" req
    (let [{:keys [user-id room-id]} (read-string (slurp (:body req)))]
      ((:dispatch chat-app) [:leave-room user-id room-id])
      {:status 200 :body "OK"}))
  
  (POST "/api/room/create" req
    (let [{:keys [room-id name description]} (read-string (slurp (:body req)))]
      ((:dispatch chat-app) [:create-room room-id name description])
      {:status 200 :body "OK"}))
  
  (GET "/api/state" []
    {:status 200
     :headers {"Content-Type" "application/edn"}
     :body (pr-str @(:app-db chat-app))})
  
  (route/not-found "Not Found"))

;; Server setup
(defn create-chat-handler []
  (-> (routes chat-routes
              (sse/sse-routes (:app-db chat-app))
              (route/resources "/" {:root "public"})
              (route/not-found "Not Found"))
      (wrap-resource "public")
      (wrap-content-type)
      (wrap-not-modified)))

(defonce server (atom nil))

(defn start-chat-server [port]
  (reset! server (http/run-server (create-chat-handler) {:port port}))
  (println "💬 Chat server started on port" port)
  
  ;; Add some initial data
  ((:dispatch chat-app) [:user-join "bot" "ChatBot"])
  ((:dispatch chat-app) [:send-message "bot" :general "Welcome to Reactor Chat!"])
  ((:dispatch chat-app) [:send-message "bot" :general "This chat is powered by server-side reactivity."]))

(defn stop-chat-server []
  (when @server
    (@server)
    (reset! server nil)))

(defn -main [& args]
  (let [port (Integer/parseInt (or (first args) "3001"))]
    (start-chat-server port)))