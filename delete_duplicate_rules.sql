-- Delete duplicate rules, keeping only the first one for each rule_id
DELETE FROM reactor_rules 
WHERE _id NOT IN (
  SELECT MIN(_id) 
  FROM reactor_rules 
  GROUP BY rule_id
);
