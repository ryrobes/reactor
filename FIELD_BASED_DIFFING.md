# Field-Based Diffing Enhancement

## Overview

Enhanced the Reactor library's diffing system from row-based to field-based diffing, with support for deep structural diffing of EDN data and nested structures.

## Changes Made

### 1. Core Diffing Enhancement (`kafka_reactive.clj`)

- **Added field-based diffing**: Only sends changed fields instead of entire rows
- **New function `compute-field-diff`**: Computes field-level differences
- **Enhanced `compute-row-diff`**: Now supports both row-based and field-based modes
- **Configuration options**: Added `field-based-diff` flag to `diff-config`

### 2. Structural Diffing Module (`structural_diff.clj` & `structural_diff.cljs`)

Created new modules for deep structural diffing:

- **EDN string parsing and diffing**: Automatically detects and parses EDN strings
- **Map diffing**: Field-level changes within nested maps
- **Sequence diffing**: Element-level changes in arrays/vectors
- **Recursive diffing**: Handles deeply nested structures

Key functions:
- `compute-structural-diff`: Deep diff between any two values
- `compute-enhanced-field-diff`: Field diff with structural support
- `apply-structural-diff`: Applies structural diffs to values
- `apply-enhanced-field-changes`: Applies field changes including structural

### 3. Client-Side Updates (`core.cljs`)

- **Enhanced diff application**: Handles both field-based and row-based diffs
- **Structural diff support**: Can apply deep structural changes
- **Message type handling**: Supports new `:field-diff-update` message type

## Configuration

The diffing behavior can be configured via `diff-config`:

```clojure
(def diff-config 
  (atom {:enabled true
         :field-based-diff true  ; Enable field-level diffing
         :max-result-size 1000   ; Don't diff if more than N rows
         :min-compression-ratio 0.7  ; Send full if diff > 70% of original
         :structure-check true}))  ; Verify structure before diffing
```

## Benefits

1. **Reduced Network Traffic**: Only changed fields are transmitted
2. **Better Performance**: Less data to serialize/deserialize
3. **Structural Data Support**: Can efficiently diff EDN blobs and nested structures
4. **Backward Compatible**: Falls back to row-based diffing when needed

## How It Works

### Field-Based Diffing Flow

1. **Server Side**:
   - Compares old and new query results
   - Identifies changed rows by ID
   - For each changed row, computes field-level differences
   - Only sends changed fields with operation type (add/update/remove)

2. **Client Side**:
   - Receives field changes
   - Applies changes to existing data
   - Handles structural updates for EDN fields

### Structural Diffing

For EDN/structural fields:
1. Parses EDN strings into data structures
2. Computes deep structural differences
3. Sends only the changed parts of the structure
4. Client applies structural patches

## Example Diff Format

### Row-Based (Legacy)
```clojure
{:type :row-diff
 :id-key :id
 :added [{:id 1 :name "New" :data "{:a 1}"}]
 :removed [2]
 :updated [{:id 3 :new-values {:id 3 :name "Updated" :data "{:a 2}"}}]}
```

### Field-Based (New)
```clojure
{:type :field-diff
 :id-key :id
 :added [{:id 1 :name "New" :data "{:a 1}"}]
 :removed [2]
 :updated [{:id 3 
            :field-changes {:name {:op :update :value "Updated"}
                          :data {:op :structural-update
                                :diff {:type :edn-diff
                                      :removed {:b 1}
                                      :added {:a 2}}}}}]}
```

## Testing

To test the new diffing:

1. Enable field-based diffing (default is on)
2. Run queries that return EDN data in fields
3. Modify data and observe the diff messages in browser console
4. Check compression ratios in server logs

## Performance Considerations

- Field-based diffing adds slight computational overhead
- Best for tables with many columns where only few change
- Structural diffing is most beneficial for large EDN blobs
- Falls back to full updates if diff is larger than original

## Future Enhancements

1. **Custom Diff Strategies**: Per-field diff strategies
2. **Binary Data Support**: Efficient binary field diffing
3. **Compression**: Additional compression for large diffs
4. **Diff Visualization**: UI components to show diffs
5. **Conflict Resolution**: Three-way merge for concurrent updates