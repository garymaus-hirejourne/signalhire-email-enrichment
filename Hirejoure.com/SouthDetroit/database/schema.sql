-- Core data model for SouthDetroit social platform
-- Ready for PostgreSQL 16 (works on MySQL with minor tweaks)

-- 1. users
CREATE TABLE users (
    id            BIGSERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    email         CITEXT UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login    TIMESTAMPTZ,
    bio           TEXT,
    location      VARCHAR(120),
    avatar_url    TEXT
);

-- 2. friendships (symmetric edge)
CREATE TABLE friendships (
    user_id   BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    friend_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status    VARCHAR(10) NOT NULL CHECK (status IN ('pending','accepted','blocked')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, friend_id)
);
CREATE INDEX idx_friendships_user_status ON friendships (user_id, status);

-- 3. posts
CREATE TABLE posts (
    id          BIGSERIAL PRIMARY KEY,
    author_id   BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body        TEXT,
    media_url   TEXT,
    privacy     VARCHAR(10) NOT NULL DEFAULT 'friends' CHECK (privacy IN ('public','friends','private')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_posts_author_created ON posts (author_id, created_at DESC);

-- 4. reactions (likes etc.)
CREATE TABLE reactions (
    post_id    BIGINT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    user_id    BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    reaction   VARCHAR(12) NOT NULL DEFAULT 'like',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (post_id, user_id)
);
CREATE INDEX idx_reactions_post ON reactions (post_id);

-- 5. comments
CREATE TABLE comments (
    id         BIGSERIAL PRIMARY KEY,
    post_id    BIGINT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    author_id  BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_comments_post_created ON comments (post_id, created_at);

-- 6. photos (metadata; blobs stored in object store/CDN)
CREATE TABLE photos (
    id          BIGSERIAL PRIMARY KEY,
    owner_id    BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    cdn_key     TEXT NOT NULL UNIQUE,
    width       INT,
    height      INT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 7. sessions (for auth tokens)
CREATE TABLE sessions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    issued_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL,
    ip          INET,
    user_agent  TEXT
);

-- End of schema
