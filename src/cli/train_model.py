from src.model_trainer import ModelTrainer

if __name__ == "__main__":
    print("Starting model training pipeline...")
    trainer = ModelTrainer()
    results = trainer.run_all_models()

    print("\n✅ Training complete.")
    print("Model performance summary:")
    print(results)
