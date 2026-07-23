import google.generativeai as genai

# Yahan apni ASLI API key daalein
genai.configure(api_key="AQ.Ab8RN6L15PeZjYQI0vQd7QtItdtGphNGKrlMOu3a5x4c3gxp5A")

print("Aapki API Key ke liye available models:")
for m in genai.list_models():
    if 'generateContent' in m.supported_generation_methods:
        print("-", m.name)