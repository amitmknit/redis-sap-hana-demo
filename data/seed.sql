-- ============================================================
-- Mock SAP HANA data — used for demo only (PostgreSQL engine)
-- Simulates: master data, pricing, employee records
-- ============================================================

-- SAP Material Master (simulates MM60 / MARA table)
CREATE TABLE material_master (
    material_id   VARCHAR(18) PRIMARY KEY,
    description   TEXT,
    plant         VARCHAR(4),
    unit_price    NUMERIC(10, 2),
    stock_qty     INTEGER,
    last_changed  TIMESTAMP DEFAULT NOW()
);

INSERT INTO material_master VALUES
  ('MAT-001', 'Industrial Pump Assembly',     'AUS1', 4250.00, 120, NOW()),
  ('MAT-002', 'Network Switch 48-port',        'AUS1',  980.00, 340, NOW()),
  ('MAT-003', 'SAP Connector Module v3',       'SYD1',  175.00,  55, NOW()),
  ('MAT-004', 'Fibre Optic Cable 100m',        'SYD1',   62.00, 820, NOW()),
  ('MAT-005', 'UPS Battery Pack 10kVA',        'MEL1', 1340.00,  30, NOW()),
  ('MAT-006', 'Wireless Access Point',         'MEL1',  420.00, 210, NOW()),
  ('MAT-007', 'Server Rack 42U',               'AUS1', 2100.00,  18, NOW()),
  ('MAT-008', 'Power Distribution Unit',       'AUS1',  340.00,  95, NOW()),
  ('MAT-009', 'Cooling Fan Assembly',          'SYD1',  185.00, 430, NOW()),
  ('MAT-010', 'SCADA Interface Card',          'MEL1', 3800.00,   8, NOW());

-- SAP Employee data (simulates SuccessFactors / HR module)
CREATE TABLE employees (
    employee_id   VARCHAR(10) PRIMARY KEY,
    full_name     TEXT,
    department    VARCHAR(50),
    cost_centre   VARCHAR(10),
    manager_id    VARCHAR(10),
    location      VARCHAR(30)
);

INSERT INTO employees VALUES
  ('EMP-0001', 'Sarah Mitchell',    'IT Operations',    'CC-101', 'EMP-0010', 'Sydney'),
  ('EMP-0002', 'James Okafor',      'Finance',          'CC-202', 'EMP-0011', 'Melbourne'),
  ('EMP-0003', 'Priya Nair',        'Procurement',      'CC-303', 'EMP-0010', 'Perth'),
  ('EMP-0004', 'Tom Berglund',      'Engineering',      'CC-404', 'EMP-0012', 'Sydney'),
  ('EMP-0005', 'Ana Reyes',         'HR',               'CC-505', 'EMP-0011', 'Brisbane'),
  ('EMP-0006', 'David Nguyen',      'IT Operations',    'CC-101', 'EMP-0010', 'Melbourne'),
  ('EMP-0007', 'Emma Walsh',        'Finance',          'CC-202', 'EMP-0011', 'Adelaide'),
  ('EMP-0008', 'Raj Patel',         'Engineering',      'CC-404', 'EMP-0012', 'Sydney'),
  ('EMP-0009', 'Fatima Al-Hassan',  'Procurement',      'CC-303', 'EMP-0010', 'Canberra'),
  ('EMP-0010', 'Chris Lawson',      'IT Leadership',    'CC-001', NULL,       'Sydney');

-- SAP Sales Orders (simulates VA03 view)
CREATE TABLE sales_orders (
    order_id      VARCHAR(12) PRIMARY KEY,
    customer_id   VARCHAR(10),
    material_id   VARCHAR(18),
    quantity      INTEGER,
    total_value   NUMERIC(12, 2),
    status        VARCHAR(20),
    created_at    TIMESTAMP DEFAULT NOW()
);

INSERT INTO sales_orders VALUES
  ('SO-100001', 'CUST-AU01', 'MAT-001',  2,  8500.00, 'DELIVERED',  NOW() - INTERVAL '5 days'),
  ('SO-100002', 'CUST-AU02', 'MAT-004', 50,  3100.00, 'IN_TRANSIT',  NOW() - INTERVAL '2 days'),
  ('SO-100003', 'CUST-AU03', 'MAT-002',  5,  4900.00, 'PROCESSING',  NOW() - INTERVAL '1 day'),
  ('SO-100004', 'CUST-AU01', 'MAT-007',  3,  6300.00, 'OPEN',        NOW()),
  ('SO-100005', 'CUST-AU04', 'MAT-010',  1,  3800.00, 'OPEN',        NOW()),
  ('SO-100006', 'CUST-AU05', 'MAT-005',  4,  5360.00, 'DELIVERED',  NOW() - INTERVAL '7 days'),
  ('SO-100007', 'CUST-AU02', 'MAT-006', 10,  4200.00, 'IN_TRANSIT',  NOW() - INTERVAL '3 days'),
  ('SO-100008', 'CUST-AU03', 'MAT-008', 20,  6800.00, 'PROCESSING',  NOW() - INTERVAL '1 day'),
  ('SO-100009', 'CUST-AU06', 'MAT-003', 15,  2625.00, 'OPEN',        NOW()),
  ('SO-100010', 'CUST-AU01', 'MAT-009', 25,  4625.00, 'DELIVERED',  NOW() - INTERVAL '4 days');

-- Reporting aggregate (simulates a heavy HANA analytical CDS view)
CREATE TABLE report_department_spend (
    report_date   DATE,
    department    VARCHAR(50),
    total_orders  INTEGER,
    total_value   NUMERIC(14, 2),
    avg_order_val NUMERIC(10, 2)
);

INSERT INTO report_department_spend VALUES
  (CURRENT_DATE, 'IT Operations',  42, 187500.00, 4464.29),
  (CURRENT_DATE, 'Engineering',    38, 312000.00, 8210.53),
  (CURRENT_DATE, 'Procurement',    61, 245800.00, 4029.51),
  (CURRENT_DATE, 'Finance',        15,  98200.00, 6546.67),
  (CURRENT_DATE, 'HR',              8,  32400.00, 4050.00);
