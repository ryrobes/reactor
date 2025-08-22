-- Delete duplicate auto-reorder rules (keep first)
DELETE FROM reactor_rules WHERE _id IN (
  'rule-443e242d-5fca-401e-bdb6-468b24179199',
  'rule-dbdff8ab-0695-4a9f-9a2f-1e74fc877851',
  'rule-f87e9dec-7fc4-4ec4-ac2a-76203a7f5ac2'
);

-- Delete duplicate calculate-daily-revenue rules (keep first)
DELETE FROM reactor_rules WHERE _id IN (
  'rule-a1413fb0-0e80-4c3d-b3db-fcd9898646ef',
  'rule-c70a8f56-bdb8-4aaa-a2d4-36ccb66e4306',
  'rule-d46658af-031c-4a10-8d10-d79a18fe9e52'
);

-- Delete duplicate low-stock-alert rules (keep first)
DELETE FROM reactor_rules WHERE _id IN (
  'rule-50ca1cb9-2978-4af4-8f8f-c6def799e574',
  'rule-68c0644a-be8e-412e-a628-1e9389c4f98b',
  'rule-73eecfaf-f179-45e3-ad19-75180d0f8b09'
);

-- Delete duplicate notify-new-purchase-order rules (keep first)
DELETE FROM reactor_rules WHERE _id IN (
  'rule-841d9b07-d8c5-41fa-a9f8-49af90d42017',
  'rule-dbc22fb7-e0c4-481e-9a7c-9309c5341757',
  'rule-edf3df8b-1bc5-4716-894f-eda46f7f8b3d'
);

-- Delete duplicate resolve-stock-alerts rules (keep first)
DELETE FROM reactor_rules WHERE _id IN (
  'rule-74b4b569-c604-47ce-bc94-7b915a9a3d18',
  'rule-adff3f23-6054-448c-8acb-0eb8b52a757d',
  'rule-f77f1a27-29b4-4a47-823c-d379c61cd77c'
);

-- Delete duplicate test-sales-alert rules (keep first)
DELETE FROM reactor_rules WHERE _id IN (
  'rule-83f3e245-2cb4-45a9-aa4c-c8b9f399ec17',
  'rule-b17cc8a3-7e37-4ce1-adf8-27475440bfbe',
  'rule-cf0112f1-9442-4739-8c5b-224b609a97fb'
);
