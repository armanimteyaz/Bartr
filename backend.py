"""Bartr backend: FastAPI + SQLAlchemy(SQLite by default) + JWT + WebSockets + optional Groq.
Run: python backend.py   (or uvicorn backend:app --reload --port 8000)"""
import os, re, json, hashlib, secrets
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List

import bcrypt
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query, Request, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from pydantic import BaseModel, Field
from sqlalchemy import (event, create_engine, Column, Integer, String, Text, DateTime, Boolean, Float,
                        ForeignKey, UniqueConstraint, Index, or_)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session, joinedload
from sqlalchemy.exc import SQLAlchemyError

try:
    from groq import Groq
except Exception:  # Groq is optional
    Groq = None

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL") or "sqlite:///./bartr.db"
JWT_SECRET = os.getenv("JWT_SECRET", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL") or "llama-3.3-70b-versatile"
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5500")
if not JWT_SECRET:
    raise SystemExit("Set JWT_SECRET in .env (see .env.example)")

if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False, "timeout": 15})

    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(conn, _):
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
else:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=280)
SessionLocal = sessionmaker(bind=engine, autoflush=False)
Base = declarative_base()
now = datetime.utcnow
iso = lambda d: d.isoformat() + "Z" if d else None


# ───────────── Models ─────────────
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(80), nullable=False)
    email = Column(String(190), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    bio = Column(Text)
    location = Column(String(120))
    timezone = Column(String(64))
    profile_picture = Column(String(500))
    github = Column(String(100))
    linkedin = Column(String(100))
    instagram = Column(String(100))
    discord = Column(String(100))
    twitter = Column(String(100))
    telegram = Column(String(100))
    created_at = Column(DateTime, default=now)
    updated_at = Column(DateTime, default=now, onupdate=now)
    skills = relationship("UserSkill", back_populates="user", cascade="all, delete-orphan")


class Skill(Base):
    __tablename__ = "skills"
    id = Column(Integer, primary_key=True)
    name = Column(String(80), nullable=False)
    normalized_name = Column(String(80), unique=True, nullable=False, index=True)
    category = Column(String(60))
    related = Column(Text)  # JSON list of related skill names (parents/tools), filled by AI or knowledge base


class UserSkill(Base):
    __tablename__ = "user_skills"
    __table_args__ = (UniqueConstraint("user_id", "skill_id", "type"), Index("ix_us_type", "type"))
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    skill_id = Column(Integer, ForeignKey("skills.id"), nullable=False, index=True)
    type = Column(String(8), nullable=False)  # teach / learn
    proficiency = Column(String(16), default="intermediate")
    years_experience = Column(Integer, default=0)
    user = relationship("User", back_populates="skills")
    skill = relationship("Skill")


class Match(Base):
    __tablename__ = "matches"
    __table_args__ = (UniqueConstraint("user_a_id", "user_b_id"),)
    id = Column(Integer, primary_key=True)
    user_a_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    user_b_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    score = Column(Float, default=0)
    reason = Column(Text)
    status = Column(String(16), default="pending")
    created_at = Column(DateTime, default=now)
    updated_at = Column(DateTime, default=now, onupdate=now)


class Conversation(Base):
    __tablename__ = "conversations"
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=now)
    updated_at = Column(DateTime, default=now, onupdate=now)
    members = relationship("ConversationMember", cascade="all, delete-orphan")


class ConversationMember(Base):
    __tablename__ = "conversation_members"
    __table_args__ = (UniqueConstraint("conversation_id", "user_id"),)
    id = Column(Integer, primary_key=True)
    conversation_id = Column(Integer, ForeignKey("conversations.id", ondelete="CASCADE"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    joined_at = Column(DateTime, default=now)
    last_read_at = Column(DateTime)


class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True)
    conversation_id = Column(Integer, ForeignKey("conversations.id", ondelete="CASCADE"), index=True)
    sender_id = Column(Integer, ForeignKey("users.id"), index=True)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=now, index=True)
    read_at = Column(DateTime)


class Review(Base):
    __tablename__ = "reviews"
    id = Column(Integer, primary_key=True)
    reviewer_id = Column(Integer, ForeignKey("users.id"), index=True)
    reviewed_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    rating = Column(Integer, nullable=False)
    comment = Column(Text)
    created_at = Column(DateTime, default=now)


class Notification(Base):
    __tablename__ = "notifications"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    type = Column(String(32))
    title = Column(String(160))
    body = Column(Text)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=now)


# ───────────── Skill knowledge (deterministic fallback) ─────────────
ALIAS = {"js": "javascript", "node": "node.js", "nodejs": "node.js", "ml": "machine learning",
         "reactjs": "react", "react.js": "react", "py": "python", "maths": "math", "calc": "calculus",
         "ui design": "ui/ux design", "ux design": "ui/ux design", "ps": "photoshop"}
DISPLAY = {"javascript": "JavaScript", "python": "Python", "node.js": "Node.js", "react": "React",
           "html": "HTML", "css": "CSS", "ui/ux design": "UI/UX Design", "sql": "SQL",
           "photoshop": "Photoshop", "typescript": "TypeScript"}
CATS = {"python": "Programming", "javascript": "Programming", "node.js": "Programming", "react": "Programming",
        "html": "Programming", "css": "Programming", "sql": "Programming", "typescript": "Programming",
        "web development": "Programming", "frontend development": "Programming",
        "backend development": "Programming", "machine learning": "Data", "data science": "Data",
        "calculus": "Math", "statistics": "Math", "algebra": "Math", "guitar": "Music", "piano": "Music",
        "singing": "Music", "photoshop": "Design", "ui/ux design": "Design", "video editing": "Design",
        "public speaking": "Communication", "writing": "Communication", "spanish": "Language",
        "french": "Language", "cooking": "Lifestyle", "yoga": "Fitness"}
RELATED = {"react": ["javascript", "web development", "frontend development"],
           "node.js": ["javascript", "web development", "backend development"],
           "javascript": ["web development", "typescript"], "html": ["web development", "frontend development"],
           "css": ["web development", "frontend development"], "typescript": ["javascript"],
           "web development": ["frontend development", "backend development"],
           "machine learning": ["python", "data science", "statistics"], "data science": ["python", "statistics", "sql"],
           "calculus": ["algebra", "math"], "statistics": ["math"], "piano": ["music theory"], "guitar": ["music theory"],
           "photoshop": ["ui/ux design"], "ui/ux design": ["frontend development"]}
TRIGGERS = {"website": "web development", "websites": "web development", "web app": "web development",
            "web apps": "web development", "web": "web development", "frontend": "frontend development",
            "backend": "backend development"}
ALIAS.update({"premiere": "premiere pro", "premier pro": "premiere pro", "davinci": "davinci resolve",
              "da vinci resolve": "davinci resolve", "da vinci": "davinci resolve", "caput": "capcut", "cap cut": "capcut",
              "fcp": "final cut pro", "final cut": "final cut pro", "video edit": "video editing", "editing videos": "video editing",
              "photo shop": "photoshop", "ae": "after effects", "fl": "fl studio", "ableton": "ableton live", "logic": "logic pro"})
DISPLAY.update({"premiere pro": "Premiere Pro", "davinci resolve": "DaVinci Resolve", "capcut": "CapCut",
                "final cut pro": "Final Cut Pro", "after effects": "After Effects", "imovie": "iMovie",
                "fl studio": "FL Studio", "ableton live": "Ableton Live", "logic pro": "Logic Pro", "3d modeling": "3D Modeling"})
# Parent skill -> tools/software that teach it. Tool <-> parent = 0.85, tool <-> sibling tool = 0.4
GROUPS = {"video editing": ["premiere pro", "davinci resolve", "capcut", "final cut pro", "filmora", "imovie", "after effects"],
          "graphic design": ["photoshop", "illustrator", "canva"],
          "music production": ["fl studio", "ableton live", "logic pro"],
          "3d modeling": ["blender", "maya"]}
CATS.update({**{t: "Video" for t in GROUPS["video editing"]}, "video editing": "Video",
             "illustrator": "Design", "canva": "Design", "graphic design": "Design", "blender": "3D", "maya": "3D",
             "3d modeling": "3D", "music production": "Music", "fl studio": "Music", "ableton live": "Music", "logic pro": "Music"})
STATIC: dict = {}


def _add(a, b, w):
    for x, y in ((a, b), (b, a)):
        if STATIC.setdefault(x, {}).get(y, 0) < w:
            STATIC[x][y] = w


for _p, _tools in GROUPS.items():
    for _i, _t in enumerate(_tools):
        _add(_p, _t, 0.85)
        for _t2 in _tools[_i + 1:]:
            _add(_t, _t2, 0.4)
for _a, _bs in RELATED.items():
    for _b in _bs:
        _add(_a, _b, 0.5)
PARENTS = set(GROUPS) | {"web development", "javascript", "frontend development", "backend development", "data science"}
LV = {"beginner": 1, "intermediate": 2, "advanced": 3, "expert": 4}


def norm(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip().lower())
    if s.startswith("adobe "):
        s = s[6:]
    return ALIAS.get(s, s)


def display(s: str) -> str:
    n = norm(s)
    return DISPLAY.get(n, n.title())


def rel_weight(a: str, b: str, g: dict) -> float:
    return 1.0 if a == b else g.get(a, {}).get(b, 0.0)


def build_graph(db: Session) -> dict:
    """Static knowledge base + AI-discovered relations stored on skills.related."""
    g = {k: dict(v) for k, v in STATIC.items()}
    for sk in db.query(Skill).filter(Skill.related.isnot(None)):
        try:
            rels = json.loads(sk.related or "[]")
        except ValueError:
            continue
        for r in rels:
            r = norm(r)
            if r and r != sk.normalized_name:
                for x, y in ((sk.normalized_name, r), (r, sk.normalized_name)):
                    if g.setdefault(x, {}).get(y, 0) < 0.6:
                        g[x][y] = 0.6
    return g


def expand_skill(name: str):
    """Ask Groq for the category and related parents/tools of a new skill. Validated, optional."""
    ai = groq_json('For the skill given, return JSON {"category":"one or two words","related":[up to 6 lowercase names '
                   'of the broader field(s) it belongs to, or well-known software/tools used for it]}. '
                   'Only real, well-known skills. Example: "Video Editing" -> related '
                   '["premiere pro","davinci resolve","capcut","final cut pro"].', name)
    if not ai:
        return None, []
    cat = ai.get("category")
    cat = cat.strip().title() if isinstance(cat, str) and re.fullmatch(r"[\w &/-]{2,30}", cat.strip()) else None
    rel = []
    for r in (ai.get("related") or [])[:6]:
        if isinstance(r, str) and re.fullmatch(r"[\w .+#/&-]{2,40}", r.strip()) and norm(r) != norm(name):
            rel.append(norm(r))
    return cat, rel


def get_skill(db: Session, name: str, category: Optional[str] = None) -> Skill:
    n = norm(name)
    sk = db.query(Skill).filter_by(normalized_name=n).first()
    if not sk:
        cat, rel = (None, [])
        if n not in CATS and n not in STATIC:
            cat, rel = expand_skill(display(name))
        sk = Skill(name=display(name), normalized_name=n, category=category or CATS.get(n) or cat or "General",
                   related=json.dumps(rel))
        db.add(sk)
        db.flush()
    return sk


# ───────────── Groq (optional) ─────────────
def groq_json(system: str, user: str):
    if not (Groq and GROQ_API_KEY and GROQ_MODEL):
        return None
    try:
        r = Groq(api_key=GROQ_API_KEY, timeout=15).chat.completions.create(
            model=GROQ_MODEL, temperature=0.3, max_tokens=5000, response_format={"type": "json_object"},
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}])
        return json.loads(r.choices[0].message.content)
    except Exception:
        return None


def analyze_text(text: str) -> List[dict]:
    """Turn free text into skill suggestions. Output is validated: every skill must appear in the
    text or be a known relative of one that does, so nothing is invented for the user."""
    low = text.lower()
    found = {}
    for k in list(CATS) + list(ALIAS) + list(TRIGGERS):
        if re.search(r"(?<![\w.])" + re.escape(k) + r"(?![\w])", low):
            n = TRIGGERS.get(k) or norm(k)
            found[n] = "mentioned"
    if not found:  # treat comma/and separated phrases as skill names
        for part in re.split(r",|;|\band\b|\n", text):
            if 1 < len(part.strip()) <= 40:
                found[norm(part)] = "mentioned"
    allowed = set(found)
    for n in list(found):
        allowed.update(STATIC.get(n, {}))
    out = dict.fromkeys(found, "mentioned")
    ai = groq_json('Extract skills from the text. Return JSON {"skills":["Skill name", ...]} with at most 10 '
                   'short skill names, including directly implied parent areas (e.g. React implies JavaScript, '
                   'Web Development). Do not add unrelated skills.', text[:600])
    if ai and isinstance(ai.get("skills"), list):
        for s in ai["skills"][:10]:
            if isinstance(s, str) and 1 < len(s.strip()) <= 40 and re.fullmatch(r"[\w .+#/&-]+", s.strip()):
                n = norm(s)
                if n in allowed or n in low:
                    out.setdefault(n, "ai")
    else:  # fallback: add related parents deterministically
        for n in list(found):
            for r in STATIC.get(n, {}):
                if r in PARENTS:
                    out.setdefault(r, "related")
    return [{"name": display(n), "category": CATS.get(n, "General"), "source": s} for n, s in out.items()][:10]


# ───────────── Matching ─────────────
def skill_map(u: User, t: str) -> dict:
    return {us.skill.normalized_name: us for us in u.skills if us.type == t}


def pairs(teach: dict, learn: dict, g: dict):
    out = []
    for l in learn:
        best = None
        for t in teach:
            w = rel_weight(t, l, g)
            if w and (not best or w > best[2]):
                best = (l, t, w)
        if best:
            out.append(best)
    return out


def tz_offset(tz: Optional[str]) -> Optional[float]:
    try:
        return ZoneInfo(tz).utcoffset(now()).total_seconds() / 3600 if tz else None
    except Exception:
        return None


def completeness(u: User) -> float:
    return sum(bool(x) for x in [u.bio, u.location, u.timezone, u.profile_picture]) / 4


def compute(a: User, b: User, g: dict) -> dict:
    ta, la, tb, lb = skill_map(a, "teach"), skill_map(a, "learn"), skill_map(b, "teach"), skill_map(b, "learn")
    ab, ba = pairs(ta, lb, g), pairs(tb, la, g)  # A teaches what B wants / B teaches what A wants
    cov = lambda p, n: sum(x[2] for x in p) / n if n else 0
    cab, cba = cov(ab, len(lb)), cov(ba, len(la))
    skill = (cab + cba) / 2 if ab and ba else max(cab, cba) * 0.5
    lvls = [min(1, LV.get(ta[t].proficiency, 2) / 4) for _, t, _ in ab] + \
           [min(1, LV.get(tb[t].proficiency, 2) / 4) for _, t, _ in ba]
    prof = sum(lvls) / len(lvls) if lvls else 0
    ca = {us.skill.category for us in a.skills}
    cb = {us.skill.category for us in b.skills}
    interest = len(ca & cb) / len(ca | cb) if ca | cb else 0
    oa, ob = tz_offset(a.timezone), tz_offset(b.timezone)
    tz = max(0, 1 - abs(oa - ob) / 12) if oa is not None and ob is not None else 0.4
    comp = (completeness(a) + completeness(b)) / 2
    score = round(100 * (0.60 * skill + 0.15 * prof + 0.10 * interest + 0.10 * tz + 0.05 * comp))
    return {"score": score, "a_teaches": ab, "b_teaches": ba, "reciprocal": bool(ab and ba)}


def facts_reason(me: User, other: User, c: dict, me_is_a: bool) -> str:
    mine, theirs = (c["a_teaches"], c["b_teaches"]) if me_is_a else (c["b_teaches"], c["a_teaches"])
    nm = other.name.split()[0]
    dn = lambda n: display(n)
    parts = []
    if mine:
        parts.append("you can teach " + ", ".join(dn(t) if t == l else f"{dn(t)} (related to {dn(l)})" for l, t, _ in mine)
                     + f" while {nm} wants to learn it")
    if theirs:
        parts.append(f"{nm} can teach " + ", ".join(dn(t) if t == l else f"{dn(t)} (related to {dn(l)})" for l, t, _ in theirs)
                     + " while you want to learn it")
    if not parts:
        return f"You and {nm} share interests but have no direct skill overlap yet."
    kind = "a strong match" if c["reciprocal"] else "a one-way match"
    return f"You and {nm} are {kind} because " + ", and ".join(parts) + "."


REASON_CACHE = {}


def explain(me: User, other: User, c: dict, me_is_a: bool) -> str:
    base = facts_reason(me, other, c, me_is_a)
    key = hashlib.md5(base.encode()).hexdigest()
    if key not in REASON_CACHE:
        ai = groq_json('Rewrite the verified fact into one friendly sentence (max 40 words) addressed to "you". '
                       'Use ONLY the facts given. Return JSON {"text":"..."}.', base)
        t = ai.get("text") if ai else None
        REASON_CACHE[key] = t.strip() if isinstance(t, str) and 10 < len(t) < 400 else base
    return REASON_CACHE[key]


def notify(db: Session, uid: int, typ: str, title: str, body: str):
    db.add(Notification(user_id=uid, type=typ, title=title, body=body))


# ───────────── Socials & uploads ─────────────
SOCIALS = ["github", "linkedin", "instagram", "discord", "twitter", "telegram"]
HANDLE_RE = re.compile(r"^[\w.\-#]{1,64}$")
URL_RE = re.compile(r"^https?://[^\s<>\"']{4,95}$", re.I)


def clean_social(field: str, v: Optional[str]) -> Optional[str]:
    v = (v or "").strip()
    if not v:
        return None
    if "://" in v:
        if field == "discord" or not URL_RE.match(v):
            raise HTTPException(422, f"{field.title()}: enter a username or a full https:// link.")
        return v
    v = v.lstrip("@").strip("/")
    if not HANDLE_RE.match(v):
        raise HTTPException(422, f"{field.title()}: use a username (letters, numbers, . _ -) or a full https:// link.")
    return v


UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)
MAX_PIC = 2 * 1024 * 1024


def sniff_image(b: bytes) -> Optional[str]:
    if b[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if b[:3] == b"\xff\xd8\xff":
        return "jpg"
    if b[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return "webp"
    return None


def drop_old_picture(u: "User"):
    p = u.profile_picture or ""
    if p.startswith("/uploads/"):
        try:
            os.remove(os.path.join(UPLOAD_DIR, os.path.basename(p)))
        except OSError:
            pass


# ───────────── Serializers ─────────────
def rating_of(db: Session, uid: int):
    rs = [r.rating for r in db.query(Review).filter_by(reviewed_user_id=uid).all()]
    return (round(sum(rs) / len(rs), 1) if rs else None), len(rs)


def sk_out(us: UserSkill):
    return {"skill_id": us.skill_id, "name": us.skill.name, "category": us.skill.category,
            "proficiency": us.proficiency, "years_experience": us.years_experience}


def user_out(db: Session, u: User, private=False):
    avg, n = rating_of(db, u.id)
    d = {"id": u.id, "name": u.name, "bio": u.bio, "location": u.location, "timezone": u.timezone,
         "profile_picture": u.profile_picture, "rating": avg, "review_count": n,
         "socials": {k: getattr(u, k) for k in SOCIALS if getattr(u, k)},
         "teach": [sk_out(s) for s in u.skills if s.type == "teach"],
         "learn": [sk_out(s) for s in u.skills if s.type == "learn"]}
    if private:
        d["email"] = u.email
    return d


def match_out(db, m: Match, me: User, other: User, c: dict):
    return {"id": m.id, "score": m.score, "status": m.status, "reciprocal": c["reciprocal"],
            "reason": explain(me, other, c, True), "user": user_out(db, other)}


# ───────────── App / auth ─────────────
app = FastAPI(title="Bartr API")
app.add_middleware(CORSMiddleware, allow_origins=list({FRONTEND_URL, "http://127.0.0.1:5500", "http://localhost:5500"}),
                   allow_methods=["*"], allow_headers=["*"], allow_credentials=True)
bearer = HTTPBearer(auto_error=False)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")


@app.exception_handler(SQLAlchemyError)
async def db_error(_: Request, exc: SQLAlchemyError):
    return JSONResponse(status_code=503, content={"detail": "Database unavailable. Check that MySQL is running and DATABASE_URL is correct."})


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def make_token(uid: int) -> str:
    return jwt.encode({"sub": str(uid), "exp": now() + timedelta(hours=24)}, JWT_SECRET, algorithm="HS256")


def user_from_token(db: Session, token: Optional[str]) -> Optional[User]:
    try:
        uid = int(jwt.decode(token or "", JWT_SECRET, algorithms=["HS256"])["sub"])
    except (JWTError, ValueError, KeyError):
        return None
    return db.get(User, uid)


def current_user(cred: Optional[HTTPAuthorizationCredentials] = Depends(bearer), db: Session = Depends(get_db)) -> User:
    u = user_from_token(db, cred.credentials if cred else None)
    if not u:
        raise HTTPException(401, "Please log in again.")
    return u


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class RegisterIn(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    email: str = Field(max_length=190)
    password: str = Field(min_length=8, max_length=72)


class LoginIn(BaseModel):
    email: str
    password: str


class ProfileIn(BaseModel):
    name: Optional[str] = Field(None, min_length=2, max_length=80)
    bio: Optional[str] = Field(None, max_length=600)
    github: Optional[str] = Field(None, max_length=100)
    linkedin: Optional[str] = Field(None, max_length=100)
    instagram: Optional[str] = Field(None, max_length=100)
    discord: Optional[str] = Field(None, max_length=100)
    twitter: Optional[str] = Field(None, max_length=100)
    telegram: Optional[str] = Field(None, max_length=100)
    location: Optional[str] = Field(None, max_length=120)
    timezone: Optional[str] = Field(None, max_length=64)
    profile_picture: Optional[str] = Field(None, max_length=500)


class SkillIn(BaseModel):
    name: str = Field(min_length=1, max_length=60)
    category: Optional[str] = Field(None, max_length=60)


class UserSkillsIn(BaseModel):
    names: List[str] = Field(min_length=1, max_length=20)
    type: str = Field(pattern="^(teach|learn)$")
    proficiency: str = Field("intermediate", pattern="^(beginner|intermediate|advanced|expert)$")
    years_experience: int = Field(0, ge=0, le=60)


class AnalyzeIn(BaseModel):
    text: str = Field(min_length=2, max_length=600)


class ReviewIn(BaseModel):
    rating: int = Field(ge=1, le=5)
    comment: Optional[str] = Field(None, max_length=1000)


class ConvIn(BaseModel):
    user_id: int


def hash_pw(p: str) -> str:
    return bcrypt.hashpw(p.encode()[:72], bcrypt.gensalt()).decode()


@app.post("/api/register", status_code=201)
def register(body: RegisterIn, db: Session = Depends(get_db)):
    email = body.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(422, "Enter a valid email address.")
    if db.query(User).filter_by(email=email).first():
        raise HTTPException(409, "An account with this email already exists.")
    u = User(name=body.name.strip(), email=email, password_hash=hash_pw(body.password), timezone="UTC")
    db.add(u)
    db.commit()
    return {"token": make_token(u.id), "user": user_out(db, u, True)}


@app.post("/api/login")
def login(body: LoginIn, db: Session = Depends(get_db)):
    u = db.query(User).filter_by(email=body.email.strip().lower()).first()
    if not u or not bcrypt.checkpw(body.password.encode()[:72], u.password_hash.encode()):
        raise HTTPException(401, "Incorrect email or password.")
    return {"token": make_token(u.id), "user": user_out(db, u, True)}


@app.get("/api/me")
def me(u: User = Depends(current_user), db: Session = Depends(get_db)):
    return user_out(db, u, True)


@app.get("/api/users/{uid}")
def get_user(uid: int, _: User = Depends(current_user), db: Session = Depends(get_db)):
    u = db.get(User, uid)
    if not u:
        raise HTTPException(404, "User not found.")
    return user_out(db, u)


@app.put("/api/users/me")
def update_me(body: ProfileIn, u: User = Depends(current_user), db: Session = Depends(get_db)):
    for k, v in body.model_dump(exclude_unset=True).items():
        if k == "timezone" and v and tz_offset(v) is None:
            raise HTTPException(422, "Unknown timezone. Use a name like Asia/Kolkata or Europe/London.")
        if k in SOCIALS:
            v = clean_social(k, v)
        if k == "profile_picture" and v and not (v.startswith("/uploads/") or URL_RE.match(v)):
            raise HTTPException(422, "Profile picture must be an uploaded image or an https:// link.")
        setattr(u, k, v.strip() if isinstance(v, str) else v)
    db.commit()
    return user_out(db, u, True)


@app.post("/api/users/me/picture")
async def upload_picture(file: UploadFile = File(...), u: User = Depends(current_user), db: Session = Depends(get_db)):
    data = await file.read(MAX_PIC + 1)
    if len(data) > MAX_PIC:
        raise HTTPException(413, "Image is too large. The maximum size is 2 MB.")
    ext = sniff_image(data)
    if not ext:
        raise HTTPException(422, "Use a PNG, JPG, GIF or WebP image.")
    name = f"{u.id}_{secrets.token_hex(8)}.{ext}"
    with open(os.path.join(UPLOAD_DIR, name), "wb") as fh:
        fh.write(data)
    drop_old_picture(u)
    u.profile_picture = "/uploads/" + name
    db.commit()
    return user_out(db, u, True)


@app.delete("/api/users/me/picture")
def delete_picture(u: User = Depends(current_user), db: Session = Depends(get_db)):
    drop_old_picture(u)
    u.profile_picture = None
    db.commit()
    return user_out(db, u, True)


# ───────────── Skills ─────────────
@app.get("/api/skills")
def list_skills(q: str = "", _: User = Depends(current_user), db: Session = Depends(get_db)):
    qs = db.query(Skill)
    if q:
        qs = qs.filter(Skill.normalized_name.like(f"%{norm(q)}%"))
    return [{"id": s.id, "name": s.name, "category": s.category} for s in qs.order_by(Skill.name).limit(50)]


@app.post("/api/skills", status_code=201)
def create_skill(body: SkillIn, _: User = Depends(current_user), db: Session = Depends(get_db)):
    s = get_skill(db, body.name, body.category)
    db.commit()
    return {"id": s.id, "name": s.name, "category": s.category}


@app.post("/api/skills/analyze")
def analyze(body: AnalyzeIn, _: User = Depends(current_user)):
    return {"suggestions": analyze_text(body.text), "ai": bool(GROQ_API_KEY and GROQ_MODEL and Groq)}


@app.post("/api/users/me/skills")
def add_my_skills(body: UserSkillsIn, u: User = Depends(current_user), db: Session = Depends(get_db)):
    for name in body.names:
        if not name.strip() or len(name) > 60:
            continue
        s = get_skill(db, name)
        us = db.query(UserSkill).filter_by(user_id=u.id, skill_id=s.id, type=body.type).first()
        if us:
            us.proficiency, us.years_experience = body.proficiency, body.years_experience
        else:
            db.add(UserSkill(user_id=u.id, skill_id=s.id, type=body.type, proficiency=body.proficiency,
                             years_experience=body.years_experience))
    db.commit()
    db.refresh(u)
    return user_out(db, u, True)


@app.delete("/api/users/me/skills/{skill_id}")
def remove_my_skill(skill_id: int, type: str = Query(pattern="^(teach|learn)$"),
                    u: User = Depends(current_user), db: Session = Depends(get_db)):
    n = db.query(UserSkill).filter_by(user_id=u.id, skill_id=skill_id, type=type).delete()
    if not n:
        raise HTTPException(404, "Skill not on your profile.")
    db.commit()
    db.refresh(u)
    return user_out(db, u, True)


# ───────────── Matches / discover ─────────────
def load_users(db: Session):
    return db.query(User).options(joinedload(User.skills).joinedload(UserSkill.skill)).all()


@app.get("/api/matches")
def get_matches(u: User = Depends(current_user), db: Session = Depends(get_db)):
    out = []
    g = build_graph(db)
    for o in load_users(db):
        if o.id == u.id:
            continue
        c = compute(u, o, g)
        if c["score"] < 25 or not (c["a_teaches"] or c["b_teaches"]):
            continue
        a, b = sorted([u.id, o.id])
        m = db.query(Match).filter_by(user_a_id=a, user_b_id=b).first()
        me_is_a = a == u.id
        if not m:
            m = Match(user_a_id=a, user_b_id=b, score=c["score"])
            db.add(m)
            db.flush()
            m.reason = facts_reason(u, o, c, me_is_a)
            if c["score"] >= 80:
                notify(db, u.id, "match", f"New strong match: {o.name}", f"{c['score']}% compatible with you.")
                notify(db, o.id, "match", f"New strong match: {u.name}", f"{c['score']}% compatible with you.")
        else:
            m.score = c["score"]
        if m.status == "rejected":
            continue
        out.append(match_out(db, m, u, o, c))
    db.commit()
    return sorted(out, key=lambda x: -x["score"])


def get_match_for(db, mid, u):
    m = db.get(Match, mid)
    if not m or u.id not in (m.user_a_id, m.user_b_id):
        raise HTTPException(404, "Match not found.")
    return m


@app.get("/api/matches/{mid}")
def match_detail(mid: int, u: User = Depends(current_user), db: Session = Depends(get_db)):
    m = get_match_for(db, mid, u)
    o = db.get(User, m.user_b_id if m.user_a_id == u.id else m.user_a_id)
    return match_out(db, m, u, o, compute(u, o, build_graph(db)))


@app.post("/api/matches/{mid}/accept")
def accept(mid: int, u: User = Depends(current_user), db: Session = Depends(get_db)):
    m = get_match_for(db, mid, u)
    m.status = "accepted"
    oid = m.user_b_id if m.user_a_id == u.id else m.user_a_id
    notify(db, oid, "match", f"{u.name} accepted your match", "Say hello and plan your first swap.")
    db.commit()
    return {"status": m.status}


@app.post("/api/matches/{mid}/reject")
def reject(mid: int, u: User = Depends(current_user), db: Session = Depends(get_db)):
    m = get_match_for(db, mid, u)
    m.status = "rejected"
    db.commit()
    return {"status": m.status}


@app.get("/api/discover")
def discover(skill: str = "", location: str = "", proficiency: str = "",
             u: User = Depends(current_user), db: Session = Depends(get_db)):
    out = []
    g = build_graph(db)
    sk = norm(skill) if skill else ""
    for o in load_users(db):
        if o.id == u.id:
            continue
        if location and location.lower() not in (o.location or "").lower():
            continue
        if sk or proficiency:
            if not any((not sk or sk in us.skill.normalized_name) and (not proficiency or us.proficiency == proficiency)
                       for us in o.skills):
                continue
        d = user_out(db, o)
        d["compatibility"] = compute(u, o, g)["score"] if u.skills and o.skills else None
        out.append(d)
    return sorted(out, key=lambda x: -(x["compatibility"] or 0))


# ───────────── Chat ─────────────
ROOMS: dict = {}     # conversation_id -> {user_id: websocket}
ONLINE: dict = {}    # user_id -> connection count


def member_of(db, cid, uid):
    return db.query(ConversationMember).filter_by(conversation_id=cid, user_id=uid).first()


def msg_out(m: Message):
    return {"id": m.id, "conversation_id": m.conversation_id, "sender_id": m.sender_id, "content": m.content,
            "created_at": iso(m.created_at), "read_at": iso(m.read_at)}


@app.get("/api/conversations")
def conversations(u: User = Depends(current_user), db: Session = Depends(get_db)):
    out = []
    for cm in db.query(ConversationMember).filter_by(user_id=u.id).all():
        other = db.query(ConversationMember).filter(ConversationMember.conversation_id == cm.conversation_id,
                                                    ConversationMember.user_id != u.id).first()
        if not other:
            continue
        ou = db.get(User, other.user_id)
        last = db.query(Message).filter_by(conversation_id=cm.conversation_id).order_by(Message.id.desc()).first()
        unread = db.query(Message).filter(Message.conversation_id == cm.conversation_id,
                                          Message.sender_id != u.id, Message.read_at.is_(None)).count()
        out.append({"id": cm.conversation_id, "user": {"id": ou.id, "name": ou.name, "profile_picture": ou.profile_picture},
                    "online": ONLINE.get(ou.id, 0) > 0, "unread": unread,
                    "last_message": msg_out(last) if last else None,
                    "updated_at": iso(last.created_at) if last else None})
    return sorted(out, key=lambda c: c["updated_at"] or "", reverse=True)


@app.post("/api/conversations", status_code=201)
def create_conversation(body: ConvIn, u: User = Depends(current_user), db: Session = Depends(get_db)):
    if body.user_id == u.id or not db.get(User, body.user_id):
        raise HTTPException(404, "User not found.")
    mine = {cm.conversation_id for cm in db.query(ConversationMember).filter_by(user_id=u.id)}
    for cm in db.query(ConversationMember).filter_by(user_id=body.user_id):
        if cm.conversation_id in mine:
            return {"id": cm.conversation_id}
    c = Conversation()
    db.add(c)
    db.flush()
    db.add_all([ConversationMember(conversation_id=c.id, user_id=u.id), ConversationMember(conversation_id=c.id, user_id=body.user_id)])
    db.commit()
    return {"id": c.id}


@app.get("/api/conversations/{cid}/messages")
def get_messages(cid: int, u: User = Depends(current_user), db: Session = Depends(get_db)):
    cm = member_of(db, cid, u.id)
    if not cm:
        raise HTTPException(403, "You are not part of this conversation.")
    db.query(Message).filter(Message.conversation_id == cid, Message.sender_id != u.id,
                             Message.read_at.is_(None)).update({"read_at": now()})
    cm.last_read_at = now()
    db.commit()
    return [msg_out(m) for m in db.query(Message).filter_by(conversation_id=cid).order_by(Message.id).limit(500)]


@app.websocket("/ws/{cid}")
async def ws_chat(ws: WebSocket, cid: int, token: str = ""):
    db = SessionLocal()
    try:
        u = user_from_token(db, token)
        if not u:
            await ws.close(code=4401)
            return
        if not member_of(db, cid, u.id):
            await ws.close(code=4403)
            return
        db.commit()
        await ws.accept()
        room = ROOMS.setdefault(cid, {})
        room[u.id] = ws
        ONLINE[u.id] = ONLINE.get(u.id, 0) + 1
        try:
            while True:
                data = await ws.receive_text()
                try:
                    content = (json.loads(data).get("content") or "").strip()
                except (ValueError, AttributeError):
                    content = data.strip()
                if not content:
                    continue
                if len(content) > 2000:
                    await ws.send_json({"type": "error", "detail": "Messages are limited to 2000 characters."})
                    continue
                others = [i for i in room if i != u.id]
                m = Message(conversation_id=cid, sender_id=u.id, content=content, read_at=now() if others else None)
                db.add(m)
                if not others:
                    for cm in db.query(ConversationMember).filter(ConversationMember.conversation_id == cid,
                                                                  ConversationMember.user_id != u.id):
                        notify(db, cm.user_id, "message", f"New message from {u.name}", content[:120])
                db.query(Conversation).filter_by(id=cid).update({"updated_at": now()})
                db.commit()
                payload = {"type": "message", **msg_out(m)}
                for sock in list(room.values()):
                    await sock.send_json(payload)
        except WebSocketDisconnect:
            pass
        finally:
            room.pop(u.id, None)
            ONLINE[u.id] = max(0, ONLINE.get(u.id, 1) - 1)
    finally:
        db.close()


# ───────────── Notifications / reviews ─────────────
@app.get("/api/notifications")
def notifications(u: User = Depends(current_user), db: Session = Depends(get_db)):
    ns = db.query(Notification).filter_by(user_id=u.id).order_by(Notification.id.desc()).limit(50).all()
    return {"unread": sum(1 for n in ns if not n.is_read),
            "items": [{"id": n.id, "type": n.type, "title": n.title, "body": n.body, "is_read": n.is_read,
                       "created_at": iso(n.created_at)} for n in ns]}


@app.post("/api/notifications/{nid}/read")
def read_notification(nid: int, u: User = Depends(current_user), db: Session = Depends(get_db)):
    n = db.query(Notification).filter_by(id=nid, user_id=u.id).first()
    if not n:
        raise HTTPException(404, "Notification not found.")
    n.is_read = True
    db.commit()
    return {"ok": True}


@app.post("/api/users/{uid}/reviews", status_code=201)
def create_review(uid: int, body: ReviewIn, u: User = Depends(current_user), db: Session = Depends(get_db)):
    if uid == u.id:
        raise HTTPException(422, "You can't review yourself.")
    if not db.get(User, uid):
        raise HTTPException(404, "User not found.")
    db.add(Review(reviewer_id=u.id, reviewed_user_id=uid, rating=body.rating, comment=(body.comment or "").strip()))
    notify(db, uid, "review", f"{u.name} left you a {body.rating}-star review", (body.comment or "")[:120])
    db.commit()
    return {"ok": True}


@app.get("/api/users/{uid}/reviews")
def list_reviews(uid: int, _: User = Depends(current_user), db: Session = Depends(get_db)):
    avg, n = rating_of(db, uid)
    rs = db.query(Review).filter_by(reviewed_user_id=uid).order_by(Review.id.desc()).all()
    return {"average": avg, "count": n, "items": [
        {"id": r.id, "rating": r.rating, "comment": r.comment, "created_at": iso(r.created_at),
         "reviewer": (db.get(User, r.reviewer_id).name if db.get(User, r.reviewer_id) else "Unknown")} for r in rs]}


@app.get("/favicon.png", include_in_schema=False)
def favicon():
    from fastapi.responses import FileResponse
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "favicon.png")
    if not os.path.exists(p):
        raise HTTPException(404, "favicon.png not found")
    return FileResponse(p)


@app.get("/api/health")
def health():
    return {"ok": True, "groq": bool(GROQ_API_KEY and GROQ_MODEL and Groq)}


# ───────────── Demo seed ─────────────
DEMO = [
    ("Zayn Abbasi", "zayn@demo.com", "Full-stack dev who wants to get better at math and music.", "Mumbai, India", "Asia/Kolkata",
     [("Python", "expert"), ("JavaScript", "advanced")], ["Calculus", "Guitar"]),
    ("Sarah Rizvi", "sarah@demo.com", "Math tutor and weekend guitarist, learning to code.", "Dubai, UAE", "Asia/Dubai",
     [("Calculus", "expert"), ("Guitar", "advanced")], ["Python", "JavaScript"]),
    ("Maya Chen", "maya@demo.com", "Designer picking up front-end skills.", "Singapore", "Asia/Singapore",
     [("Photoshop", "expert"), ("UI/UX Design", "advanced")], ["React", "Video Editing"]),
    ("Zoe Martins", "zoe@demo.com", "Short-form creator who edits everything in CapCut.", "Lisbon, Portugal", "Europe/Lisbon",
     [("CapCut", "expert")], ["Photoshop", "Public Speaking"]),
    ("Daniel Okoye", "daniel@demo.com", "Speaker coach and data nerd.", "Lagos, Nigeria", "Africa/Lagos",
     [("Public Speaking", "expert"), ("Statistics", "advanced")], ["Machine Learning", "Piano"]),
    ("Priya Nair", "priya@demo.com", "ML engineer who loves web projects.", "Bengaluru, India", "Asia/Kolkata",
     [("Machine Learning", "advanced"), ("Python", "advanced")], ["Web Development", "Video Editing"]),
    ("Liam Brown", "liam@demo.com", "Video editor (Premiere Pro, DaVinci) learning piano and Python.", "London, UK", "Europe/London",
     [("Premiere Pro", "advanced"), ("DaVinci Resolve", "advanced")], ["Piano", "Python"]),
]


def seed_demo():
    db = SessionLocal()
    try:
        old = db.query(User).filter_by(email="alex@demo.com").first()  # rename demo users in older databases
        if old:
            old.name, old.email = "Zayn Abbasi", "zayn@demo.com"
            s = db.query(User).filter_by(email="sarah@demo.com").first()
            if s:
                s.name = "Sarah Rizvi"
            db.commit()
        if db.query(User).filter_by(email="zayn@demo.com").first():
            return
        pw = hash_pw("demo1234")
        for name, email, bio, loc, tz, teach, learn in DEMO:
            u = User(name=name, email=email, password_hash=pw, bio=bio, location=loc, timezone=tz,
                     profile_picture=f"https://api.dicebear.com/7.x/thumbs/svg?seed={name.split()[0]}")
            db.add(u)
            db.flush()
            for s, lv in teach:
                db.add(UserSkill(user_id=u.id, skill_id=get_skill(db, s).id, type="teach", proficiency=lv, years_experience=4))
            for s in learn:
                db.add(UserSkill(user_id=u.id, skill_id=get_skill(db, s).id, type="learn", proficiency="beginner"))
        db.commit()
        print("Seeded demo users (password: demo1234)")
    finally:
        db.close()


def migrate():
    """Add columns introduced after a database was first created (SQLite/MySQL safe)."""
    from sqlalchemy import inspect, text
    insp = inspect(engine)
    want = {"users": {k: "VARCHAR(100)" for k in SOCIALS}, "skills": {"related": "TEXT"}}
    with engine.begin() as cx:
        for table, cols in want.items():
            have = {c["name"] for c in insp.get_columns(table)}
            for name, typ in cols.items():
                if name not in have:
                    cx.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {typ}"))


@app.on_event("startup")
def startup():
    try:
        Base.metadata.create_all(engine)
        migrate()
        seed_demo()
    except SQLAlchemyError as e:
        print(f"\n[!] MySQL unavailable: {e.__class__.__name__}. API will return 503 until it is reachable.\n")


if __name__ == "__main__":
    uvicorn.run("backend:app", host="0.0.0.0", port=8000, reload=False)