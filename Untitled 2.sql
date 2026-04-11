CREATE AUTHENTICATION POLICY enforce_password_mfa
-- Require MFA enrollment when logging in with username and password
MFA_ENROLLMENT = REQUIRED;

ALTER ACCOUNT SET AUTHENTICATION POLICY enforce_password_mfa;