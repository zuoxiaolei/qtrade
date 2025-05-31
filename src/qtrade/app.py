import datetime
import json
import uuid
from typing import List, Optional, Dict, Any

import uvicorn
from fastapi import FastAPI, Request, HTTPException, Form, Path, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
from datetime import timezone, timedelta


CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
FRONT_DIRECTORY = os.path.abspath(os.path.join(CURRENT_DIRECTORY, os.pardir, "frontend"))

def beijing_now():
    return datetime.datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8)))

app = FastAPI(title="动态内容管理系统")
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # 允许的源列表
#     allow_credentials=True,  # 允许携带凭证（如 cookies）
#     allow_methods=["*"],  # 允许所有方法（GET, POST 等）
#     allow_headers=["*"],  # 允许所有头部
# )
app.mount("/static", StaticFiles(directory=FRONT_DIRECTORY), name="static")
templates = Jinja2Templates(directory=FRONT_DIRECTORY)

@app.get("/", response_class=HTMLResponse)
async def admin_root(request: Request):
    return RedirectResponse(url="/admin", status_code=302)


@app.get("/admin", response_class=HTMLResponse)
async def admin_interface(request: Request):
    return FileResponse(os.path.join(FRONT_DIRECTORY, "admin.html"))



# from pydantic import BaseModel, Field
# from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
# from sqlalchemy.orm import sessionmaker, Session
# from sqlalchemy.ext.declarative import declarative_base
# from urllib.parse import quote
# from fastapi.middleware.cors import CORSMiddleware
# from datetime import timezone, timedelta

# # --- Database Setup (SQLAlchemy with SQLite) ---
# DATABASE_URL = "sqlite:///./data.db"
# engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Base = declarative_base()

# def beijing_now():
#     return datetime.datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8)))

# # --- SQLAlchemy Models ---
# class PageDB(Base):
#     __tablename__ = "pages"
#     id = Column(Integer, primary_key=True, index=True)
#     share_slug = Column(String, unique=True, index=True, default=lambda: str(uuid.uuid4()))
#     title = Column(String, index=True, default="未命名页面")
#     created_at = Column(DateTime, default=beijing_now)
#     updated_at = Column(DateTime, default=beijing_now, onupdate=beijing_now)
#     # Content will be stored as JSON strings
#     header_config_json = Column(Text, default="{}") # For main title, subtitle, image, etc.
#     modules_config_json = Column(Text, default="[]") # For dynamic content modules
#     footer_config_json = Column(Text, default="{}") # For footer text

# Base.metadata.create_all(bind=engine)

# # --- Pydantic Schemas (for API validation and response) ---
# class ModuleSectionItem(BaseModel):
#     id: str = Field(default_factory=lambda: str(uuid.uuid4()))
#     text: str

# class ModuleSection(BaseModel):
#     id: str = Field(default_factory=lambda: str(uuid.uuid4()))
#     title: str = "技术亮点"
#     icon: str = "lightbulb" # Material Icon name
#     items: List[ModuleSectionItem] = Field(default_factory=list)

# class PageModule(BaseModel):
#     id: str = Field(default_factory=lambda: str(uuid.uuid4()))
#     module_title: str = "新模块标题"
#     module_link: Optional[str] = None
#     module_description: Optional[str] = "" # Not in example HTML but good to have
#     sections: List[ModuleSection] = Field(default_factory=lambda: [ModuleSection(), 
#                                                                    ModuleSection(title="应用场景", icon="widgets"),
#                                                                     ModuleSection(title="对企知道存在哪些启发", icon="auto_awesome")])

# class HeaderConfig(BaseModel):
#     header_image_url: str = "https://lh3.googleusercontent.com/aida-public/AB6AXuDXZgrMD8ThDvsyPXTHSIq8J3xUTl_LF4bS7SRwQZmqTpSojeIX9v8voGtIh42eXys62fz47Pqvrx-9QpoXKjNo5Unbh0G5Z-9Rri-0X2KDUmZ6Vlgdkbf3aOJ42YNxLxQw8E0tkR2tsyXPpG77oD-DeK494NjBQ1j4T5Jn3C2pj3cfhiOd8FRpr0VkFAqL9ivpQj-N8G6_wQN0YWmTfZdLbAx-3iBdHAqdcqwl3DmgzabY2LEesBksgrmSTkEoxNZCxur6_EA2qXo"
#     main_title: str = "AI 前沿"
#     main_title_date_suffix: bool = True # To add (YYMMDD)
#     sub_title: str = "探索前沿科技，赋能未来创新"
#     department_info: str = "from 企知道算法部"

# class FooterConfig(BaseModel):
#     text: str = f">© {datetime.datetime.now().year} 企知道算法部. 保留所有权利."

# class PageCreate(BaseModel):
#     title: str = "AI前沿"
#     header_config: HeaderConfig = Field(default_factory=HeaderConfig)
#     modules_config: List[PageModule] = Field(default_factory=lambda: [PageModule()])
#     footer_config: FooterConfig = Field(default_factory=FooterConfig)

# class PageUpdate(PageCreate):
#     pass

# class PageDisplay(PageCreate):
#     id: int
#     share_slug: str
#     created_at: datetime.datetime
#     updated_at: datetime.datetime

#     class Config:
#         orm_mode = True # Changed from from_attributes for Pydantic v2
        
#     # Custom resolver to parse JSON strings from DB
#     @classmethod
#     def from_orm_custom(cls, db_page: PageDB):
#         return cls(
#             id=db_page.id,
#             share_slug=db_page.share_slug,
#             title=db_page.title,
#             created_at=db_page.created_at,
#             updated_at=db_page.updated_at,
#             header_config=HeaderConfig(**json.loads(db_page.header_config_json or "{}")),
#             modules_config=[PageModule(**module_data) for module_data in json.loads(db_page.modules_config_json or "[]")],
#             footer_config=FooterConfig(**json.loads(db_page.footer_config_json or "{}"))
#         )


# # --- FastAPI App Setup ---
# app = FastAPI(title="动态内容管理系统")
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # 允许的源列表
#     allow_credentials=True,  # 允许携带凭证（如 cookies）
#     allow_methods=["*"],  # 允许所有方法（GET, POST 等）
#     allow_headers=["*"],  # 允许所有头部
# )
# app.mount("/static", StaticFiles(directory="static"), name="static")
# templates = Jinja2Templates(directory="templates")

# # Dependency to get DB session
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# # --- Helper function to get page or 404 ---
# def get_page_or_404(page_id: int, db: Session) -> PageDB:
#     page = db.query(PageDB).filter(PageDB.id == page_id).first()
#     if not page:
#         raise HTTPException(status_code=404, detail="页面未找到")
#     return page

# def get_page_by_slug_or_404(slug: str, db: Session) -> PageDB:
#     page = db.query(PageDB).filter(PageDB.share_slug == slug).first()
#     if not page:
#         raise HTTPException(status_code=404, detail="页面未找到")
#     return page
    
# # --- API Endpoints ---
# @app.post("/api/pages", response_model=PageDisplay, status_code=201)
# async def create_page_api(page_data: PageCreate, db: Session = Depends(get_db)):
#     db_page = PageDB(
#         title=page_data.title,
#         header_config_json=page_data.header_config.model_dump_json(),
#         modules_config_json=json.dumps([m.model_dump() for m in page_data.modules_config]),
#         footer_config_json=page_data.footer_config.model_dump_json(),
#         share_slug=str(uuid.uuid4()) # Ensure unique slug
#     )
#     db.add(db_page)
#     db.commit()
#     db.refresh(db_page)
#     return PageDisplay.from_orm_custom(db_page)

# @app.get("/api/pages", response_model=List[PageDisplay])
# async def list_pages_api(db: Session = Depends(get_db)):
#     pages = db.query(PageDB).order_by(PageDB.updated_at.desc()).all()
#     return [PageDisplay.from_orm_custom(p) for p in pages]

# @app.get("/api/pages/{page_id}", response_model=PageDisplay)
# async def get_page_api(page_id: int = Path(..., title="页面ID"), db: Session = Depends(get_db)):
#     db_page = get_page_or_404(page_id, db)
#     return PageDisplay.from_orm_custom(db_page)

# @app.put("/api/pages/{page_id}", response_model=PageDisplay)
# async def update_page_api(page_data: PageUpdate, page_id: int = Path(..., title="页面ID"), db: Session = Depends(get_db)):
#     db_page = get_page_or_404(page_id, db)
#     db_page.title = page_data.title
#     db_page.header_config_json = page_data.header_config.model_dump_json()
#     db_page.modules_config_json = json.dumps([m.model_dump() for m in page_data.modules_config])
#     db_page.footer_config_json = page_data.footer_config.model_dump_json()
#     db_page.updated_at = beijing_now()
#     db.commit()
#     db.refresh(db_page)
#     return PageDisplay.from_orm_custom(db_page)

# @app.delete("/api/pages/{page_id}", status_code=204)
# async def delete_page_api(page_id: int = Path(..., title="页面ID"), db: Session = Depends(get_db)):
#     db_page = get_page_or_404(page_id, db)
#     db.delete(db_page)
#     db.commit()
#     return None

# # --- Frontend Routes ---
# @app.get("/", response_class=HTMLResponse)
# async def admin_root(request: Request):
#     return RedirectResponse(url="/admin", status_code=302)

# @app.get("/admin", response_class=HTMLResponse)
# async def admin_interface(request: Request):
#     return templates.TemplateResponse("admin.html", {"request": request})

# @app.get("/view/{share_slug}", response_class=HTMLResponse)
# async def view_shared_page(
#     request: Request, 
#     share_slug: str = Path(..., title="分享链接标识"), 
#     db: Session = Depends(get_db)
# ):
#     db_page = get_page_by_slug_or_404(share_slug, db)
#     page_data = PageDisplay.from_orm_custom(db_page)
    
#     # Prepare data for template
#     display_main_title = page_data.header_config.main_title
#     if page_data.header_config.main_title_date_suffix:
#         display_main_title += f" ({datetime.datetime.now().strftime('%y%m%d')})"

#     return templates.TemplateResponse("rendered_page.html", {
#         "request": request,
#         "page_title": page_data.title,
#         "header": {
#             "image_url": page_data.header_config.header_image_url,
#             "main_title": display_main_title,
#             "sub_title": page_data.header_config.sub_title,
#             "department_info": page_data.header_config.department_info,
#         },
#         "modules": page_data.modules_config,
#         "footer_text": page_data.footer_config.text
#     })

# @app.get("/export/{share_slug}/html")
# async def export_page_as_html(
#     share_slug: str = Path(..., title="分享链接标识"), 
#     db: Session = Depends(get_db)
# ):
#     db_page = get_page_by_slug_or_404(share_slug, db)
#     page_data = PageDisplay.from_orm_custom(db_page)
    
#     display_main_title = page_data.header_config.main_title
#     if page_data.header_config.main_title_date_suffix:
#         display_main_title += f" ({datetime.datetime.now().strftime('%y%m%d')})"

#     # Render the template to a string first
#     html_content = templates.get_template("rendered_page.html").render({
#         "request": None, # No request object needed for string rendering
#         "page_title": page_data.title,
#         "header": {
#             "image_url": page_data.header_config.header_image_url,
#             "main_title": display_main_title,
#             "sub_title": page_data.header_config.sub_title,
#             "department_info": page_data.header_config.department_info,
#         },
#         "modules": page_data.modules_config,
#         "footer_text": page_data.footer_config.text
#     })
    
#     return HTMLResponse(
#         content=html_content,
#         headers={"Content-Disposition": f"attachment; filename={share_slug}_{quote(page_data.title.replace(' ','_'))}.html"}
#     )

# # PDF and Image export are more complex. 
# # Image export is handled client-side in rendered_page.html using html2canvas.
# # PDF export would ideally use a server-side library like WeasyPrint or Playwright/Puppeteer,
# # which are heavier dependencies. For now, we'll omit server-side PDF.

if __name__ == "__main__":
    print("启动 FastAPI 应用，请访问 http://127.0.0.1:8000/admin")
    uvicorn.run(app, host="0.0.0.0", port=8018)