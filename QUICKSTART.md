# Reactor - Quick Start Guide

## 🚀 Getting Started in 60 Seconds

### 1. Start the Server
```bash
# Run the todo app server
lein run -m examples.todo-app.server 3000
```

The server is now running at http://localhost:3000 with:
- HTML interface at `/`
- API state at `/api/state`
- Event dispatch at `/api/dispatch`
- SSE subscriptions at `/subscribe`

### 2. Test the API

```bash
# View current state
curl http://localhost:3000/api/state

# Add a todo
echo '[:add-todo "My new task"]' | curl -X POST http://localhost:3000/api/dispatch \
  -H "Content-Type: application/edn" --data-binary @-

# Toggle a todo
echo '[:toggle-todo 1]' | curl -X POST http://localhost:3000/api/dispatch \
  -H "Content-Type: application/edn" --data-binary @-

# Subscribe to changes (SSE)
curl http://localhost:3000/subscribe?path=todos&format=edn
```

### 3. Build the Client (Optional)
```bash
# Install dependencies
npm install

# Start development build
npm run dev

# Open http://localhost:8080 in your browser
```

## 📚 Library Usage

### Basic Reactive Atom
```clojure
(require '[reactor.core :as r])

; Create a reactive atom
(def app-state (r/ratom {:count 0}))

; Subscribe to changes
(r/subscribe! app-state [:count]
  (fn [old new]
    (println "Count changed from" old "to" new)))

; Update state (triggers subscription)
(swap! app-state update :count inc)
```

### Server-Side Re-frame
```clojure
(require '[reactor.frame :as rf])

; Create an app
(def app (rf/create-frame-app {:todos {}}))

; Register event handler
(rf/reg-event-db :add-todo
  (fn [db [text]]
    (assoc-in db [:todos (random-uuid)] {:text text})))

; Dispatch event
((:dispatch app) [:add-todo "Learn Reactor"])
```

### Rules Engine
```clojure
; Define reactive business rules
(r/def-rule app-state :auto-save [:data]
  (fn [data] (> (count data) 10))  ; condition
  (fn [_ data]                     ; action
    (persist-to-disk! data)))
```

### Time-Based Reactions
```clojure
; Create a time atom
(def timer (r/time-ratom {:interval :minute}))

; React to time changes
(r/subscribe! timer [:minute]
  (fn [_ minute]
    (when (zero? (mod minute 5))
      (println "5 minutes have passed!"))))
```

## 🏗️ Architecture

```
Your App
    ↓
Reactor Frame (Re-frame for server)
    ↓
Reactor Core (Reactive atoms, rules, subscriptions)
    ↓
Reactor SSE (Real-time client sync)
```

## 🎯 Key Features

- **Reactive State**: Atoms that trigger on change
- **Smart Subscriptions**: Only fire on relevant changes
- **Cascading Rules**: Business logic that chains automatically
- **Time as Data**: Reactive scheduling
- **SSE Transport**: Real-time without WebSockets
- **Server Authority**: Single source of truth
- **Re-frame Compatible**: Familiar patterns

## 📖 Examples

Check out the example applications:
- `src/examples/todo_app/` - Full todo application
- `src/examples/chat_app/` - Real-time chat
- `src/examples/basic_usage.clj` - Core concepts

## 🧪 Testing

```bash
# Run core tests
lein test

# Test the server
./test_server.sh
```

## 🚢 Production

```bash
# Build uberjar
lein uberjar

# Run in production
java -jar target/reactor-0.1.0-SNAPSHOT-standalone.jar
```

## 💡 Tips

1. **Start Small**: Begin with basic ratoms before adding rules
2. **Use Cursors**: For efficient nested data access
3. **Lazy Subscriptions**: Use `:lazy true` for expensive computations
4. **Debug Mode**: Enable logging to see subscription fires
5. **Batch Updates**: Group related changes in single swaps

## 🔗 Resources

- [Functional Requirements](FR.md)
- [Architecture Overview](README_APPS.md)
- [API Documentation](docs/api.md) *(coming soon)*

## 🤝 Contributing

Reactor is open for contributions! Areas of interest:
- Performance optimizations
- Additional examples
- Client libraries for other platforms
- Persistence backends

---

**Ready to build reactive Clojure applications? Start with the todo app and explore from there!**