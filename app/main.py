from fastapi import FastAPI

# Create the FastAPI app instance
app = FastAPI(
    title="Sagepick Core",
    description="Sagepick Core Backend API and Services",
    version="1.0.0"
)

# Root endpoint
@app.get("/")
def read_root():
    return {"name": "Sagepick Core Backend!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
