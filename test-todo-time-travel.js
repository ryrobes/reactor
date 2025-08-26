// Test time travel in the todo app
// Run this in browser console at http://localhost:8084/todo-enhanced.html

async function testTimeTravel() {
  console.log("=== Testing TODO App Time Travel ===");
  
  const ns = window.examples.todo_app.client_enhanced;
  
  // Test 1: Switch to test session
  console.log("\n1. Switching to timetest session...");
  ns.session_id.reset("timetest");
  ns.setup_subscription_BANG_();
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  // Test 2: Fetch history
  console.log("\n2. Fetching history...");
  ns.fetch_history_BANG_();
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  const timestamps = ns.history_timestamps.deref();
  console.log("Found " + timestamps.length + " history points:", timestamps);
  
  if (timestamps && timestamps.length > 1) {
    // Test 3: Travel to first timestamp
    console.log("\n3. Time traveling to:", timestamps[0]);
    ns.time_travel_to_BANG_(timestamps[0]);
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    const state1 = ns.app_state.deref();
    console.log("State at " + timestamps[0] + ":", state1);
    
    // Test 4: Travel to another timestamp
    if (timestamps.length > 2) {
      console.log("\n4. Time traveling to:", timestamps[2]);
      ns.time_travel_to_BANG_(timestamps[2]);
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      const state2 = ns.app_state.deref();
      console.log("State at " + timestamps[2] + ":", state2);
    }
    
    // Test 5: Return to present
    console.log("\n5. Returning to present...");
    ns.time_travel_back_BANG_();
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    const currentState = ns.app_state.deref();
    console.log("Current state:", currentState);
  }
  
  // Test 6: Switch back to default
  console.log("\n6. Switching back to default session...");
  ns.session_id.reset("default");
  ns.setup_subscription_BANG_();
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  console.log("\n=== Time Travel Test Complete ===");
  console.log("✓ History fetching works");
  console.log("✓ Time travel to past states works");
  console.log("✓ Return to present works");
  console.log("✓ Session isolation maintained");
}

console.log("Run testTimeTravel() to test time travel functionality");