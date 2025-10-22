from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import io
import time
import json
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

processed_data_store = {}

def convert_to_serializable(obj):
    """Convert pandas Timestamp and other non-serializable objects to strings"""
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif pd.isna(obj):
        return None
    elif isinstance(obj, (int, float)):
        return float(obj) if pd.notna(obj) else None
    return obj

def process_pizza_data(file_content):
    try:
        df = pd.read_csv(io.StringIO(file_content.decode('utf-8')))
        
        df.fillna({'quantity': 1, 'total_price': df['unit_price']}, inplace=True)
        
        df['order_datetime'] = pd.to_datetime(
            df['order_date'] + ' ' + df['order_time'], 
            format='%m/%d/%Y %H:%M:%S',
            errors='coerce'
        )
        
        df = df.dropna(subset=['order_datetime'])
        
        df['month'] = df['order_datetime'].dt.month
        df['day_of_week'] = df['order_datetime'].dt.day_name()
        df['hour'] = df['order_datetime'].dt.hour
        df['revenue'] = df['quantity'] * df['total_price']
        
        return df
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

def calculate_metrics(df):
    total_revenue = df['revenue'].sum()
    total_orders = df['order_id'].nunique()
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    
    # Convert to serializable format before grouping
    df_serializable = df.copy()
    
    # Convert Timestamp columns to string for serialization
    df_serializable['order_datetime'] = df_serializable['order_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    popular_pizzas = df.groupby('pizza_name').agg({
        'quantity': 'sum',
        'revenue': 'sum'
    }).sort_values('quantity', ascending=False).head(10).reset_index()
    
    category_sales = df.groupby('pizza_category')['revenue'].sum().reset_index()
    
    size_sales = df.groupby('pizza_size')['revenue'].sum().reset_index()
    
    daily_sales = df.groupby(df['order_datetime'].dt.date)['revenue'].sum().reset_index()
    daily_sales.columns = ['date', 'revenue']
    daily_sales['date'] = daily_sales['date'].astype(str)
    
    hourly_sales = df.groupby('hour')['revenue'].sum().reset_index()
    
    # Convert data preview to serializable format
    data_preview = []
    for _, row in df.head(10).iterrows():
        serializable_row = {}
        for col, value in row.items():
            serializable_row[col] = convert_to_serializable(value)
        data_preview.append(serializable_row)
    
    # Convert metrics to native Python types
    return {
        "total_revenue": float(round(total_revenue, 2)),
        "total_orders": int(total_orders),
        "avg_order_value": float(round(avg_order_value, 2)),
        "total_items_sold": int(df['quantity'].sum()),
        "popular_pizzas": popular_pizzas.to_dict('records'),
        "category_sales": category_sales.to_dict('records'),
        "size_sales": size_sales.to_dict('records'),
        "daily_sales": daily_sales.to_dict('records'),
        "hourly_sales": hourly_sales.to_dict('records'),
        "data_preview": data_preview
    }

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    try:
        content = await file.read()
        processed_df = process_pizza_data(content)
        
        file_id = str(int(time.time()))
        
        processed_data_store[file_id] = {
            'filename': file.filename,
            'dataframe': processed_df,
            'record_count': len(processed_df),
            'metrics': calculate_metrics(processed_df)
        }
        
        return JSONResponse({
            "message": "File processed successfully",
            "file_id": file_id,
            "record_count": len(processed_df),
            "metrics": processed_data_store[file_id]['metrics']
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/{file_id}")
async def get_metrics(file_id: str):
    if file_id not in processed_data_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    return processed_data_store[file_id]['metrics']

@app.get("/api/raw-data/{file_id}")
async def get_raw_data(file_id: str, limit: int = 50):
    if file_id not in processed_data_store:
        raise HTTPException(status_code=404, detail="File not found")
    
    df = processed_data_store[file_id]['dataframe']
    
    # Convert to serializable format
    serializable_data = []
    for _, row in df.head(limit).iterrows():
        serializable_row = {}
        for col, value in row.items():
            serializable_row[col] = convert_to_serializable(value)
        serializable_data.append(serializable_row)
    
    return serializable_data

@app.get("/api/uploaded-files")
async def get_uploaded_files():
    files_info = {}
    for file_id, file_data in processed_data_store.items():
        files_info[file_id] = {
            'filename': file_data['filename'],
            'record_count': file_data['record_count'],
            'upload_time': file_id
        }
    return files_info

@app.get("/")
async def health_check():
    return {"status": "ready", "message": "Pizza Analytics API is running!"}