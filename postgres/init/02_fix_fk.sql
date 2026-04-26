-- Drop FK constraint if it exists (migration safety)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fraud_decisions_transaction_id_fkey'
    ) THEN
        ALTER TABLE fraud_decisions DROP CONSTRAINT fraud_decisions_transaction_id_fkey;
    END IF;
END $$;
