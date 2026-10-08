# Bartr Backend

A FastAPI backend for **Bartr**, a skill-exchange platform where users
can list skills they can teach and skills they want to learn, discover
compatible people, match with them, chat in real time, leave reviews,
and receive notifications.

## Tech Stack

-   **Python**
-   **FastAPI**
-   **Uvicorn**
-   **SQLAlchemy**
-   **SQLite** by default, with MySQL-compatible configuration
-   **JWT** authentication
-   **bcrypt** password hashing
-   **WebSockets** for real-time chat
-   **Pydantic** request validation
-   **Groq** AI integration (optional)
-   **python-dotenv**
-   Static profile-image uploads

The application creates its database tables automatically on startup and
runs a lightweight migration for newer columns.

## Features

### Authentication

-   User registration and login
-   JWT access tokens valid for 24 hours
-   Password hashing with bcrypt
-   Authenticated `/api/me` endpoint

### Profiles

Users can manage: - Name - Bio - Location - Timezone - Profile picture -
GitHub - LinkedIn - Instagram - Discord - Twitter/X - Telegram

Profile pictures: - PNG, JPG, GIF, and WebP - Maximum size: **2 MB** -
Old uploaded profile pictures are removed when replaced

### Skills

Users can maintain two skill lists:

-   **Teach** --- skills they can teach
-   **Learn** --- skills they want to learn

Each skill can have: - Category - Proficiency: beginner / intermediate /
advanced / expert - Years of experience

The backend also normalizes common aliases such as: - `js` →
JavaScript - `node` / `nodejs` → Node.js - `py` → Python - `reactjs` →
React - `premiere` → Premiere Pro - `davinci` → DaVinci Resolve -
`capcut` / `cap cut` → CapCut

### AI Skill Analysis

`/api/skills/analyze` can analyze free-form text and suggest skills.

Groq is optional. If Groq is unavailable, the backend falls back to its
deterministic skill knowledge base.

The AI integration is also used to: - Discover relationships for
previously unknown skills - Generate friendly explanations for verified
match reasons

AI output is constrained and validated rather than being blindly
accepted.

### Skill Matching

Bartr calculates compatibility using:

  Factor                          Weight
  ----------------------------- --------
  Skill compatibility                60%
  Relevant proficiency               15%
  Shared categories/interests        10%
  Timezone compatibility             10%
  Profile completeness                5%

A match can be: - Reciprocal --- both users can teach each other -
One-way --- only one useful teaching direction exists

Matches below the backend's minimum compatibility threshold are excluded
from the normal match list.

### Discover

Users can discover other users by: - Skill - Location - Proficiency

Results can also include a calculated compatibility score.

### Real-Time Chat

The backend provides: - Conversations - Conversation members -
Persistent messages - Read status - Unread message counts - Online
presence - WebSocket real-time messaging - Message length limit of 2000
characters

WebSocket endpoint:

``` text
/ws/{conversation_id}?token=<JWT>
```

### Notifications

Notifications are generated for events such as: - Strong matches -
Accepted matches - New messages - Reviews

Users can retrieve notifications and mark individual notifications as
read.

### Reviews

Users can: - Rate another user from 1--5 - Add an optional comment -
View average rating - View review count - View individual reviews

Users cannot review themselves.

## Project Structure

``` text
project/
├── backend.py
├── .env
├── .env.example
├── bartr.db              # Created automatically with SQLite
├── uploads/              # Created automatically
└── favicon.png           # Optional
```

## Requirements

Create a `requirements.txt` containing:

``` txt
fastapi
uvicorn[standard]
sqlalchemy
bcrypt
python-dotenv
python-jose
pydantic
python-multipart
groq
pymysql
```

`groq` is optional if you do not need AI functionality. `pymysql` is
only required when using MySQL.

## Installation

### 1. Create a virtual environment

Windows:

``` bat
python -m venv venv
venv\Scripts\activate
```

Linux/macOS:

``` bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install dependencies

``` bash
pip install -r requirements.txt
```

## Environment Variables

Create a `.env` file in the same directory as `backend.py`:

``` env
JWT_SECRET=replace-this-with-a-long-random-secret

DATABASE_URL=sqlite:///./bartr.db

FRONTEND_URL=http://localhost:5500

# Optional Groq configuration
GROQ_API_KEY=
GROQ_MODEL=llama-3.3-70b-versatile
```

### Production JWT Secret

Do not use a short or predictable JWT secret.

Generate one with Python:

``` bash
python -c "import secrets; print(secrets.token_urlsafe(64))"
```

Copy the generated value into:

``` env
JWT_SECRET=YOUR_GENERATED_SECRET
```

## Database

SQLite is the default:

``` env
DATABASE_URL=sqlite:///./bartr.db
```

The backend enables: - Foreign keys - SQLite WAL mode - A 15-second
SQLite connection timeout

For MySQL, use a SQLAlchemy MySQL URL, for example:

``` env
DATABASE_URL=mysql+pymysql://USER:PASSWORD@HOST:3306/DATABASE
```

The application automatically creates its tables during startup.

## Run the Backend

### Directly

``` bash
python backend.py
```

The backend listens on:

``` text
http://0.0.0.0:8000
```

### With Uvicorn

Development:

``` bash
uvicorn backend:app --reload --port 8000
```

Production-style local run:

``` bash
uvicorn backend:app --host 0.0.0.0 --port 8000
```

## API Documentation

Once the server is running:

``` text
http://127.0.0.1:8000/docs
```

FastAPI automatically provides an interactive Swagger UI.

Alternative OpenAPI documentation:

``` text
http://127.0.0.1:8000/redoc
```

## API Overview

### Health

``` http
GET /api/health
```

Returns backend health and whether Groq is configured.

### Authentication

``` http
POST /api/register
POST /api/login
GET  /api/me
```

Authenticated requests use:

``` http
Authorization: Bearer <JWT>
```

### Profile

``` http
GET    /api/users/{uid}
PUT    /api/users/me
POST   /api/users/me/picture
DELETE /api/users/me/picture
```

### Skills

``` http
GET    /api/skills
POST   /api/skills
POST   /api/skills/analyze
POST   /api/users/me/skills
DELETE /api/users/me/skills/{skill_id}
```

### Matching

``` http
GET  /api/matches
GET  /api/matches/{match_id}
POST /api/matches/{match_id}/accept
POST /api/matches/{match_id}/reject
GET  /api/discover
```

### Conversations

``` http
GET  /api/conversations
POST /api/conversations
GET  /api/conversations/{conversation_id}/messages
```

### WebSocket Chat

``` text
/ws/{conversation_id}?token=<JWT>
```

### Notifications

``` http
GET  /api/notifications
POST /api/notifications/{notification_id}/read
```

### Reviews

``` http
POST /api/users/{uid}/reviews
GET  /api/users/{uid}/reviews
```

## Demo Data

On startup, the backend seeds demo users if the demo database has not
already been initialized.

Demo password:

``` text
demo1234
```

Demo accounts include:

``` text
zayn@demo.com
sarah@demo.com
maya@demo.com
zoe@demo.com
daniel@demo.com
priya@demo.com
liam@demo.com
```

**Do not use these demo credentials in production.**

## CORS

The backend allows the configured frontend URL plus:

``` text
http://127.0.0.1:5500
http://localhost:5500
```

Configure your deployed frontend through:

``` env
FRONTEND_URL=https://your-frontend-domain.example
```

For production, restrict allowed origins to only the domains that
actually need access.

## Uploads

Uploaded profile images are stored in:

``` text
uploads/
```

They are served through:

``` text
/uploads/<filename>
```

Maximum image size:

``` text
2 MB
```

Accepted formats:

``` text
PNG
JPG/JPEG
GIF
WebP
```

## Security Notes

-   Passwords are hashed with bcrypt.
-   Authentication uses signed JWT tokens.
-   JWT tokens expire after 24 hours.
-   Protected endpoints require authentication.
-   Uploaded images are checked by file signature rather than trusting
    only the filename.
-   Social fields are validated.
-   User-generated message content is limited to 2000 characters.
-   SQLAlchemy parameterization is used for database operations.

### Production Hardening Recommended

Before public deployment:

-   Use a strong random `JWT_SECRET`.
-   Use HTTPS.
-   Restrict CORS to the real frontend domain.
-   Use a production database such as MySQL/PostgreSQL for larger
    deployments.
-   Put FastAPI behind Nginx or another reverse proxy.
-   Run multiple application workers where appropriate.
-   Add rate limiting for authentication and sensitive endpoints.
-   Add database migrations using Alembic for larger production
    projects.
-   Store uploaded files in object storage/CDN if the application grows.
-   Do not expose demo accounts or credentials.
-   Protect and rotate API keys.

## Architecture

``` text
Frontend
   │
   ├── REST API ───────────────┐
   │                           │
   └── WebSocket Chat          ▼
                           FastAPI
                              │
                 ┌────────────┼────────────┐
                 ▼            ▼            ▼
             JWT/Auth    SQLAlchemy     Matching
                 │            │            │
                 │       SQLite/MySQL      │
                 │                         │
                 └──────────┬──────────────┘
                            ▼
                       Optional Groq
```

## Startup Flow

When the application starts:

1.  FastAPI initializes.
2.  Database tables are created if missing.
3.  Lightweight database migration checks for newer columns.
4.  Demo data is seeded when necessary.
5.  The API becomes available on port `8000`.

## Important Notes

-   The backend expects `JWT_SECRET` to be present. Startup exits if it
    is missing.
-   SQLite is the default database.
-   Groq is optional; core matching has a deterministic fallback.
-   WebSocket authentication uses the JWT token passed as the `token`
    query parameter.
-   The backend is designed to work with a separate frontend, typically
    served from port `5500`.

## License

Add your project's license here before publishing the repository.
