from fastapi import FastAPI
from pydantic import BaseModel
import torch
import torch.nn as nn
import numpy as np
import uvicorn
import joblib   # <--- load scaler


# =====================================
# MODEL DEFINITION (TrafficGRU)
# =====================================
class TrafficGRU(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_layers, output_dim, dropout=0.3):
        super().__init__()
        self.gru = nn.GRU(
            input_size=input_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout
        )

        self.bn = nn.BatchNorm1d(hidden_dim)
        self.fc1 = nn.Linear(hidden_dim, hidden_dim // 2)
        self.relu = nn.ReLU()
        self.drop = nn.Dropout(0.4)
        self.fc2 = nn.Linear(hidden_dim // 2, output_dim)

    def forward(self, x):
        out, _ = self.gru(x)
        out = out[:, -1, :]
        out = self.bn(out)
        out = self.relu(self.fc1(out))
        out = self.drop(out)
        out = self.fc2(out)
        return out


# =====================================
# CONFIG
# =====================================
WINDOW_SIZE = 5
FEATURES = 5
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# =====================================
# LOAD MODEL
# =====================================
model = TrafficGRU(
    input_dim=FEATURES,
    hidden_dim=128,
    num_layers=2,
    output_dim=3
).to(DEVICE)

print("\n===== LOADING MODEL =====")
state = torch.load("traffic_gru_weights.pth", map_location=DEVICE)
model.load_state_dict(state)
model.eval()
print("Model loaded successfully!")

# print model structure
for name, param in model.named_parameters():
    print(name, param.shape)


# =====================================
# LOAD SCALER
# =====================================
print("\n===== LOADING SCALER =====")
scaler = joblib.load("traffic_scaler.pkl")
print("Scaler mean:", scaler.mean_)
print("Scaler scale:", scaler.scale_)


# =====================================
# FASTAPI APP
# =====================================
app = FastAPI(title="Traffic GRU Prediction API")


# =====================================
# INPUT FORMAT
# =====================================
class WindowInput(BaseModel):
    data: list  # list 5x5


# =====================================
# PREDICTION FUNCTION (WITH DEBUG)
# =====================================
def predict_window(window):
    arr = np.array(window)

    print("\n================ RAW INPUT ================")
    print(arr)

    # Scale
    arr_scaled = scaler.transform(arr)

    print("\n================ SCALED INPUT ================")
    print(arr_scaled)
    print("Min:", arr_scaled.min(), "Max:", arr_scaled.max())

    # Convert to tensor
    x = torch.tensor(arr_scaled, dtype=torch.float32).unsqueeze(0).to(DEVICE)

    with torch.no_grad():
        out = model(x)
        probs = torch.softmax(out, dim=1).cpu().numpy()[0]

    print("\n================ MODEL PROBS ================")
    print("Class 0:", probs[0])
    print("Class 1:", probs[1])
    print("Class 2:", probs[2])

    pred = int(np.argmax(probs))
    print("===== FINAL PREDICTION:", pred, "=====")

    return pred


# =====================================
# API ENDPOINT
# =====================================
@app.post("/predict")
def predict(input_data: WindowInput):
    window = input_data.data

    if len(window) != WINDOW_SIZE:
        return {"error": f"Window must contain {WINDOW_SIZE} rows."}

    prediction = predict_window(window)

    return {
        "prediction": prediction
    }

# =====================================
# MAIN ENTRY
# =====================================
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8127,
        reload=True
    )
