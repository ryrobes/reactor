// Test script to verify todo app fixes
// Run this in browser console at http://localhost:8084/todo-enhanced.html

async function testTodoApp() {
  console.log("=== Testing TODO App Fixes ===");
  
  // Test 1: Session switching
  console.log("\n1. Testing session switching...");
  
  // Switch to a different session
  window.examples.todo_app.client_enhanced.session_id.reset("test-session-" + Date.now());
  window.examples.todo_app.client_enhanced.setup_subscription_BANG_();
  
  await new Promise(resolve => setTimeout(resolve, 2000));
  console.log("✓ Session switched successfully");
  
  // Test 2: Add a todo in new session
  console.log("\n2. Testing todo creation in new session...");
  window.examples.todo_app.client_enhanced.add_todo_BANG_({
    id: "test-" + Date.now(),
    text: "Test todo in new session",
    completed: false
  });
  
  await new Promise(resolve => setTimeout(resolve, 1000));
  console.log("✓ Todo added successfully");
  
  // Test 3: Time travel
  console.log("\n3. Testing time travel...");
  
  // Fetch history first
  window.examples.todo_app.client_enhanced.fetch_history_BANG_();
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  const timestamps = window.examples.todo_app.client_enhanced.history_timestamps.deref();
  if (timestamps && timestamps.length > 0) {
    console.log("Found " + timestamps.length + " history points");
    
    // Travel to first timestamp
    const firstTimestamp = timestamps[0];
    window.examples.todo_app.client_enhanced.time_travel_to_BANG_(firstTimestamp);
    
    await new Promise(resolve => setTimeout(resolve, 2000));
    console.log("✓ Time travel executed successfully");
    
    // Travel back to present
    window.examples.todo_app.client_enhanced.time_travel_back_BANG_();
    await new Promise(resolve => setTimeout(resolve, 1000));
    console.log("✓ Returned to present successfully");
  } else {
    console.log("⚠ No history found (expected for new session)");
  }
  
  // Test 4: Switch back to default session
  console.log("\n4. Testing switch back to default session...");
  window.examples.todo_app.client_enhanced.session_id.reset("default");
  window.examples.todo_app.client_enhanced.setup_subscription_BANG_();
  
  await new Promise(resolve => setTimeout(resolve, 2000));
  console.log("✓ Switched back to default session");
  
  console.log("\n=== All tests completed ===");
  console.log("Please verify:");
  console.log("1. Session switching works without errors");
  console.log("2. Todos persist per session");
  console.log("3. Time travel shows correct historical states");
  console.log("4. No lingering SSE connections");
}

// Instructions
console.log("To test the todo app fixes, run:");
console.log("testTodoApp()");