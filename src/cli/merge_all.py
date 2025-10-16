from src.data_merger import DataMerger

if __name__ == "__main__":
    merger = DataMerger()
    out = merger.merge_all()
    path = merger.save(out)
    print(f"✅ Merged dataset: {out.shape[0]} rows, {out.shape[1]} columns")
    print(f"💾 Saved to: {path}")

    # quick coverage check
    coverage = out.notna().mean().sort_values(ascending=False)
    print("\nNon-null coverage (fraction):")
    print(coverage.to_string())
