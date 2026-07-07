-- Autenticação da plataforma (utilizadores, permissões, meta, manutenção)
-- Base: plataforma_central (mesmas credenciais EDA_MYSQL_* no .env)

CREATE TABLE IF NOT EXISTS plataforma_users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role ENUM('admin', 'colaborador') NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    perms_version INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_plataforma_users_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS plataforma_user_permissions (
    user_id INT NOT NULL,
    tab_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (user_id, tab_id),
    CONSTRAINT fk_plataforma_user_permissions_user
        FOREIGN KEY (user_id) REFERENCES plataforma_users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS plataforma_meta (
    meta_key VARCHAR(128) PRIMARY KEY,
    meta_value MEDIUMTEXT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS plataforma_page_maintenance (
    tab_id VARCHAR(64) PRIMARY KEY,
    enabled TINYINT(1) NOT NULL DEFAULT 0,
    message TEXT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
