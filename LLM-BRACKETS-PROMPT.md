# Clojure Structure-First Development Strategy

## IMPORTANT: All tools are pre-installed and available as system commands

## CRITICAL: Never Count Brackets!
When writing or fixing Clojure code, NEVER attempt to count opening and closing brackets. This leads to cascading errors. Instead, focus ONLY on indentation to represent structure, then let tools handle the brackets.

## Available Tools

You have access to three whitelisted commands for Clojure development:

### 1. clj-kondo - Validation Only
```bash
# Check for syntax errors and structural issues
clj-kondo --lint <filename>

# Check code from stdin
echo "(defn foo [x])" | clj-kondo --lint -

# Get config-aware linting
clj-kondo --lint <filename> --config '{:output {:format :json}}'
```

### 2. parinfer-rust - Automatic Bracket Fixing
```bash
# Smart mode (recommended) - infers brackets from indentation
parinfer-rust --mode smart <filename>

# Indent mode - more aggressive, fully replaces brackets based on indentation  
parinfer-rust --mode indent <filename>

# Paren mode - adjusts indentation based on brackets (use rarely)
parinfer-rust --mode paren <filename>

# Process from stdin
cat <filename> | parinfer-rust --mode smart
```

### 3. fix-clojure-structure - Combined Workflow
```bash
# This tool is already installed and available system-wide
# Usage:
fix-clojure-structure.sh <filename.clj>

# Example:
fix-clojure-structure.sh src/app/core.clj

# What it does:
# 1. Creates a backup of your file
# 2. Analyzes current structure with clj-kondo
# 3. Applies parinfer-rust smart mode
# 4. Validates the fixed structure
# 5. If issues remain, tries indent mode
# 6. Reports success or remaining indentation issues
```

## WORKFLOW STRATEGY

### When Writing New Clojure Code:

1. **Write with indentation focus**:
   - Each nested expression indents 2 spaces from its parent
   - Align elements at the same logical level
   - Don't worry about bracket placement
   
2. **Save and auto-fix**:
   ```bash
   parinfer-rust --mode smart myfile.clj > myfile-fixed.clj
   mv myfile-fixed.clj myfile.clj
   ```

3. **Validate**:
   ```bash
   clj-kondo --lint myfile.clj
   ```

### When Fixing Bracket Errors:

**STOP! Do not count or match brackets!** Instead:

1. **First, check what the structure should be**:
   ```bash
   # Look at the error to understand the STRUCTURAL problem, not bracket count
   clj-kondo --lint broken.clj
   ```

2. **Fix the INDENTATION to match intended structure**:
   - If something should be nested inside, indent it
   - If something should be at the same level, align it
   - Ignore ALL bracket warnings while doing this

3. **Let parinfer fix the brackets**:
   ```bash
   # This will fix all brackets based on your indentation
   parinfer-rust --mode smart broken.clj > fixed.clj
   mv fixed.clj broken.clj
   ```

4. **If issues persist, use the combined tool**:
   ```bash
   fix-clojure-structure.sh broken.clj
   ```

## EXAMPLE PROBLEMS AND SOLUTIONS

### Problem 1: "Unmatched delimiter )"
```clojure
;; WRONG APPROACH: Don't count brackets!
;; RIGHT APPROACH: Fix indentation

;; Before (broken):
(defn process-data [coll]
  (map #(str % "-processed")
    (filter even? coll))  ; ← error here

;; Fix by correcting INDENTATION:
(defn process-data [coll]
  (map #(str % "-processed")
       (filter even? coll)))  ; ← aligned with map's first arg

;; Then run: parinfer-rust --mode smart file.clj > fixed.clj
```

### Problem 2: "Expected ] but got )"
```clojure
;; This means the structure is wrong, not just brackets
;; Check: Is something at the wrong nesting level?

;; Fix the INDENTATION first, then let parinfer handle brackets
```

### Problem 3: Complex nested structure
```clojure
;; When you have mixed brackets (,[ and {:
;; Focus on the LOGICAL structure:

;; Before (broken):
(let [config {:server {:port 8080
              :host "localhost"}
      logger (create-logger config)]
  (start-server config logger))  ; ← errors here

;; Fix INDENTATION to show structure:
(let [config {:server {:port 8080
                       :host "localhost"}}  ; ← align map contents
      logger (create-logger config)]
  (start-server config logger))

;; Run: fix-clojure-structure.sh file.clj
```

## GOLDEN RULES

1. **NEVER** manually count brackets
2. **NEVER** add/remove brackets to "balance" them  
3. **ALWAYS** fix structure through indentation
4. **ALWAYS** use parinfer-rust after adjusting indentation
5. If clj-kondo still reports errors after parinfer, the **indentation is wrong**, not the brackets

## Quick Commands Reference

```bash
# Quick fix attempt
parinfer-rust --mode smart file.clj > temp.clj && mv temp.clj file.clj

# Validate structure  
clj-kondo --lint file.clj

# Full fix workflow (pre-installed tool)
fix-clojure-structure.sh file.clj

# Check specific function structure
grep -A 20 "defn function-name" file.clj | parinfer-rust --mode smart

# Fix all .clj files in src directory
find src -name "*.clj" -exec fix-clojure-structure.sh {} \;

# Preview what parinfer would change (no file modification)
parinfer-rust --mode smart file.clj | diff file.clj -
```

## When Explaining Issues

Instead of saying: "There's a bracket mismatch on line 42"

Say: "The expression starting on line 42 should be nested inside the let binding above it. Fix the indentation to show this relationship."

## Common Patterns to Recognize

### Threading Macros
```clojure
;; Correct indentation for -> and ->>
(-> initial-value
    (step-one arg1)
    (step-two arg2)
    (step-three arg3))
```

### Let Bindings
```clojure
;; Each binding pair aligned, body indented
(let [x 1
      y 2
      z (+ x y)]
  (println x y z))
```

### Maps and Vectors
```clojure
;; Map keys and values aligned
{:key1 "value1"
 :key2 "value2"
 :nested {:inner-key "inner-value"
          :another "value"}}
```

## Emergency Recovery

If the code is severely broken:

1. **Remove ALL closing brackets at the end of the file**
2. **Focus only on proper indentation**
3. **Run indent mode (more aggressive)**:
   ```bash
   parinfer-rust --mode indent broken.clj > recovered.clj
   ```
4. **Validate and iterate**:
   ```bash
   clj-kondo --lint recovered.clj
   ```

## Shadow-cljs Integration

When shadow-cljs reports bracket errors:

1. **DON'T try to fix based on the error message**
2. **Instead, run**:
   ```bash
   fix-clojure-structure.sh src/main/app.cljs
   ```
3. **Then restart shadow-cljs**:
   ```bash
   npx shadow-cljs compile app
   ```

## Remember

- Brackets are a SYMPTOM
- Indentation shows STRUCTURE  
- Fix structure, brackets follow automatically
- The tools are your allies - let them do the bracket work
- Your job is to think about the logical structure and express it through indentation

When in doubt: indent correctly and run `fix-clojure-structure.sh`!


