import chromadb
from sentence_transformers import SentenceTransformer
import pandas as pd

# 1. Khởi tạo Chroma client
client = chromadb.Client()

# 2. Tạo collection
collection = client.create_collection(
    name="sightseeing",
    metadata={"hnsw:space": "cosine"}
)

# 3. Load Vietnamese embedding model
model = SentenceTransformer('intfloat/multilingual-e5-base')

# 4. Load data
data = pd.read_csv("quan1_filtered_full.csv", encoding='utf-8')

data = data.reset_index(drop=True)
data.index = data.index + 1  # Bắt đầu từ 1 thay vì 0
data = data.reset_index()  # Đẩy index thành cột
data = data.rename(columns={'index': 'id'})  # Đổi tên cột

data.reset_index
# 5. Prepare documents để embed
documents = []
metadatas = []
ids = []
embeddings = []

# ✅ CÁCH 1: Dùng iterrows() (dễ đọc)
for idx, item in data.iterrows():
    # Combine text tiếng Việt để embed
    combined_text = f"{item['name']} {item['address']} {item['comment']} {item['type']}"
    
    documents.append(combined_text)
    
    # Embed bằng Vietnamese model
    embedding = model.encode(combined_text).tolist()
    embeddings.append(embedding)
    
    # Metadata
    metadatas.append({
        "name": item["name"],
        "address": item["address"],
        "type": item["type"],
        "rating": str(item["rating"]),
        "lat": str(item["lat"]),
        "lon": str(item["lon"])
    })
    
    ids.append(str(item["id"]))  # Convert to string

# 6. Add vào collection
collection.add(
    documents=documents,
    embeddings=embeddings,
    metadatas=metadatas,
    ids=ids
)

print("✅ Đã embed tiếng Việt và lưu vào Chroma")

# 7. Query tiếng Việt
query_text = "tôi muốn tham quan vườn thú, bảo tàng"
query_embedding = model.encode(query_text).tolist()

results = collection.query(
    query_embeddings=[query_embedding],
    n_results=5
)

print("\n🔍 Kết quả tìm kiếm:")
for i, (doc, metadata, distance) in enumerate(zip(results['documents'][0],results['metadatas'][0], results['distances'][0])):
    print(f"\n{i+1}. {metadata['name']}")
    print(f"   Địa chỉ: {metadata['address']}")
    print(f"   Loại: {metadata['type']}")
    print(f"   Đánh giá: {metadata['rating']}/5")
    print(f"   doc: {doc}")
    print(f"   Độ tương tự: {(1 - distance)*100:.1f}%")