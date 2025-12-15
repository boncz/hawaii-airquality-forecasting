"""
visualizations.py
-----------------
Generates all figures for the Hawaii Air Quality Forecasting thesis project.
Figures are saved to reports/figures/ for inclusion in the thesis Results section.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import r2_score


# Set consistent plot style
sns.set(style="whitegrid", context="talk")

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
TABLES_DIR = REPORTS_DIR / "tables"
PREDICTIONS_DIR = REPORTS_DIR / "predictions"  # ✅ Added line
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# Load model performance data
# ---------------------------------------------------------------------
performance_path = TABLES_DIR / "model_performance.csv"
df = pd.read_csv(performance_path)
print(f"📂 Loading model performance from: {performance_path.resolve()}")
print(df)

# Clean up columns
df = df.rename(columns={"Unnamed: 0": "Model"})
df = df.sort_values("MAE")  # sort by MAE ascending for display

# ---------------------------------------------------------------------
# Figure 1: Model Comparison (MAE, RMSE, R²)
# ---------------------------------------------------------------------
def plot_model_comparison(df: pd.DataFrame):
    # Clean up model names for readability
    df = df.copy()
    df["Model"] = df["Model"].str.replace("_", " ").str.title()

    sns.set_style("white")

    # Make figure wider for breathing room
    fig, ax1 = plt.subplots(figsize=(13, 6))
    metrics = ["MAE", "RMSE"]
    df_melt = df.melt(id_vars="Model", value_vars=metrics,
                      var_name="Metric", value_name="Value")

    sns.barplot(data=df_melt, x="Model", y="Value", hue="Metric",
                ax=ax1, palette="Set2")

    # --- Title and subtitle ---
    ax1.set_title("Which Model Minimises Forecast Error for Hourly PM$_{2.5}$ in Hawaiʻi?",
                  fontsize=18, weight="bold", pad=15)
    ax1.text(0.5, 0.99,
         "Comparison of MAE and RMSE for LR, RF & GBR using a multi-year hourly dataset (µg/m³)",
         transform=ax1.transAxes, ha="center", fontsize=12, color="dimgray")

    # --- Axes ---
    ax1.set_xlabel("")  # redundant
    ax1.set_ylabel("Error (µg/m³)", fontsize=11)
    plt.xticks(rotation=0, ha="center", fontsize=11)
    plt.yticks(fontsize=10)
    ax1.grid(False)
    sns.despine(ax=ax1, top=True, right=True, left=False, bottom=False)

    # --- Add larger, smarter callout values ---
    for p in ax1.patches:
        height = p.get_height()
        if height > 0.05:  # avoid random 0.00 labels
            ax1.text(
                p.get_x() + p.get_width() / 2,
                height + 0.05,
                f"{height:.2f}",
                ha="center",
                va="bottom",
                fontsize=12,
                fontweight="semibold"
            )

    # --- Legend below with two columns ---
    legend = plt.legend(
        title="Metric",
        fontsize=11,
        title_fontsize=12,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.20),
        ncol=2,
        frameon=False,
    )

    # --- Padding adjustments ---
    plt.subplots_adjust(top=0.88, bottom=0.25)
    plt.tight_layout(rect=[0, 0.05, 1, 0.94])

    # --- Save figure ---
    plt.savefig(FIGURES_DIR / "figure1_model_error_comparison.png",
                dpi=300, bbox_inches="tight")
    plt.close()

# ---------------------------------------------------------------------
# Figure 2: Predicted vs Observed Scatter for Gradient Boosting
# ---------------------------------------------------------------------
def plot_predicted_vs_observed():
    preds_path = PREDICTIONS_DIR / "predictions_gradient_boosting_tuned.csv"
    if preds_path.exists():
        df_pred = pd.read_csv(preds_path)

        from sklearn.metrics import r2_score
        r2 = r2_score(df_pred["y_true"], df_pred["y_pred"])

        # --- Create figure (taller to avoid squish) ---
        fig, ax = plt.subplots(figsize=(8, 8.6))
        sns.set_style("white")

        # Scatterplot
        sns.scatterplot(
            x="y_true", y="y_pred", data=df_pred,
            s=35, alpha=0.6, color="#3b82f6", edgecolor=None, ax=ax
        )

        # 1:1 reference line
        max_val = max(df_pred["y_true"].max(), df_pred["y_pred"].max())
        ax.plot([0, max_val], [0, max_val],
                color="red", linestyle="--", linewidth=1.3, label="1:1 Line")

        # Regression trend line
        sns.regplot(
            x="y_true", y="y_pred", data=df_pred,
            scatter=False, color="gray", line_kws={"linewidth": 1.5, "alpha": 0.8},
            ci=None, label="Trend Line", ax=ax
        )

        # --- Centered title and subtitle (manual placement) ---
        fig.text(0.5, 0.985,
                 "Observed vs. Predicted PM$_{2.5}$ (Gradient Boosting)",
                 ha="center", fontsize=16, weight="bold")
        fig.text(0.5, 0.955,
                 "Test set predictions compared with observed hourly EPA PM$_{2.5}$ values",
                 ha="center", fontsize=12, color="dimgray")

        # Axes
        ax.set_xlabel("Observed PM$_{2.5}$ (µg/m³)", fontsize=12)
        ax.set_ylabel("Predicted PM$_{2.5}$ (µg/m³)", fontsize=12)
        ax.set_xlim(0, max_val + 2)
        ax.set_ylim(0, max_val + 2)
        ax.tick_params(axis="x", labelsize=11)
        ax.tick_params(axis="y", labelsize=11)

        # R² annotation
        ax.text(0.05 * max_val, 0.9 * max_val, f"$R^2$ = {r2:.2f}",
                fontsize=12, weight="semibold", color="black")

        # Legend
        ax.legend(frameon=False, loc="upper left", fontsize=10)

        sns.despine(ax=ax)

        # --- Slightly looser layout ---
        plt.tight_layout(rect=[0, 0, 1, 0.93])
        plt.savefig(FIGURES_DIR / "figure2_predicted_vs_observed_gradient_boosting.png",
                    dpi=300, bbox_inches="tight")
        plt.close()



def plot_all_model_scatters():
    """
    Creates a 2x3 grid of observed vs predicted scatterplots for all models.
    """
    sns.set_style("white")

    # Define models to include
    model_names = [
        "linear_regression",
        "random_forest",
        "gradient_boosting_tuned",
        "epa_correction",
        "lrpa_correction"
    ]

    # Create subplots grid
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    axes = axes.flatten()

    for i, name in enumerate(model_names):
        ax = axes[i]
        preds_path = PREDICTIONS_DIR / f"predictions_{name}.csv"  # ✅ Fixed path
        if not preds_path.exists():
            ax.axis("off")
            ax.text(0.5, 0.5, f"No data for\n{name.title()}", ha="center", va="center")
            continue

        # Load predictions
        df_pred = pd.read_csv(preds_path)
        r2 = r2_score(df_pred["y_true"], df_pred["y_pred"])
        max_val = max(df_pred["y_true"].max(), df_pred["y_pred"].max())

        # Scatter points
        sns.scatterplot(
            x="y_true", y="y_pred", data=df_pred,
            s=30, alpha=0.6, color="#3b82f6", ax=ax, edgecolor=None
        )

        # 1:1 line
        ax.plot([0, max_val], [0, max_val], color="red", linestyle="--", linewidth=1)

        # Trend line
        sns.regplot(
            x="y_true", y="y_pred", data=df_pred,
            scatter=False, color="gray", line_kws={"linewidth": 1.3, "alpha": 0.8},
            ci=None, ax=ax
        )

        # Formatting
        ax.set_title(name.replace("_", " ").title(), fontsize=12, weight="bold", pad=6)
        ax.set_xlabel("Observed PM$_{2.5}$ (µg/m³)", fontsize=10)
        ax.set_ylabel("Predicted PM$_{2.5}$ (µg/m³)", fontsize=10)
        ax.set_xlim(0, max_val + 2)
        ax.set_ylim(0, max_val + 2)
        ax.text(0.05 * max_val, 0.9 * max_val, f"$R^2$ = {r2:.2f}",
                fontsize=10, weight="semibold", color="black")
        sns.despine(ax=ax)

    # Turn off any unused subplot (if fewer than 6)
    for j in range(len(model_names), len(axes)):
        axes[j].axis("off")

    # Shared title
    plt.suptitle("How Well Do Each of the Models Predict Hourly PM$_{2.5}$?",
                 fontsize=16, weight="bold", y=1.02)
    plt.figtext(0.5, 0.96,
                "Observed vs Predicted PM$_{2.5}$ (µg/m³) for each model\n"
                "Red dashed = 1:1 line | Gray = trend line | $R^2$ annotated per plot",
                ha="center", fontsize=11, color="dimgray")

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(FIGURES_DIR / "figure2_model_comparison_scattergrid.png",
                dpi=300, bbox_inches="tight")
    plt.close()

# ---------------------------------------------------------------------
# Figure 3: R² Comparison Highlight
# ---------------------------------------------------------------------
def plot_r2_highlight(df: pd.DataFrame):
    # Clean and format model names
    name_map = {
        "linear_regression": "Linear Regression",
        "random_forest": "Random Forest",
        "gradient_boosting_tuned": "Gradient Boosting (Tuned)",
        "epa_correction": "EPA Correction",
        "lrpa_correction": "LRAPA Correction"
    }
    df = df.copy()
    df["Model"] = df["Model"].replace(name_map)
    df = df.sort_values("R2", ascending=True)

    # Create figure (slightly taller for balance)
    fig, ax = plt.subplots(figsize=(9, 6))

    # Color scheme: blue = positive, red = negative
    colors = df["R2"].apply(lambda x: "#2563eb" if x >= 0 else "#dc2626")

    # --- Plot bars ---
    bars = ax.barh(df["Model"], df["R2"], color=colors, alpha=0.9)
    ax.axvline(0, color="black", linewidth=1)

    # --- Manual title and subtitle positioning for perfect alignment ---
    fig.text(0.5, 0.98,
             "How Well Does Each Model Explain Hourly PM$_{2.5}$ Variability?",
             ha="center", fontsize=16, weight="bold")
    fig.text(0.5, 0.945,
             "Positive R² values indicate better performance than a mean baseline; "
             "negative R² values indicate worse performance.",
             ha="center", fontsize=11, color="dimgray")

    # --- Axes ---
    ax.set_xlabel("R² (Coefficient of Determination)", fontsize=11)
    ax.set_ylabel("")
    ax.set_xlim(-1.3, 0.3)
    ax.tick_params(axis="x", labelsize=10)
    ax.tick_params(axis="y", labelsize=11)
    sns.despine(left=True, bottom=True)

    # --- Numeric callouts ---
    for bar, val in zip(bars, df["R2"]):
        x = bar.get_width()
        ha = "left" if val >= 0 else "right"
        offset = 0.03 if val >= 0 else -0.03
        ax.text(
            x + offset,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.2f}",
            va="center",
            ha=ha,
            fontsize=10,
            fontweight="semibold",
            color="black"
        )

    # --- Layout ---
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(FIGURES_DIR / "figure3_r2_highlight.png", dpi=300, bbox_inches="tight")
    plt.close()



def save_performance_table_image(df):
    import matplotlib.pyplot as plt

    df_display = df.copy()
    df_display["Model"] = df_display["Model"].str.replace("_", " ").str.title()

    fig, ax = plt.subplots(figsize=(9, 1.7))
    ax.axis("off")

    table = ax.table(
        cellText=df_display.values,
        colLabels=df_display.columns,
        loc="center",
        cellLoc="center"
    )

    # Styling
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.2, 1.6)

    # Bold header row
    for key, cell in table.get_celld().items():
        if key[0] == 0:  # Header row
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#EFEFEF")  

    plt.savefig(FIGURES_DIR / "table_model_performance.png",
                dpi=300, bbox_inches="tight")
    plt.close()
    print("✅ Saved model performance table image.")



def plot_missing_values(df, title="Missing Values by Column"):
    """Bar chart of missing value counts per column."""
    plt.figure(figsize=(10,4))
    df.isnull().sum().sort_values(ascending=False).plot.bar(color="gray")
    plt.title(title)
    plt.ylabel("Count of Missing Values")
    plt.tight_layout()
    plt.show()


def plot_scatter_aqs_vs_purpleair(df, title="AQS vs PurpleAir (Hourly)"):
    """Scatter plot comparing hourly AQS and PurpleAir PM2.5."""
    subset = df.dropna(subset=["pm25_aqs", "pm25_purpleair"])
    plt.figure(figsize=(6,6))
    plt.scatter(subset["pm25_aqs"], subset["pm25_purpleair"], alpha=0.3, color="slateblue")
    plt.plot([0, subset["pm25_aqs"].max()], [0, subset["pm25_aqs"].max()], 'k--', lw=1)
    plt.xlabel("AQS PM₂.₅ (µg/m³)")
    plt.ylabel("PurpleAir PM₂.₅ (µg/m³)")
    plt.title(title)
    plt.tight_layout()
    plt.show()


def plot_hourly_vs_daily(df_hourly, df_daily):
    """Overlays hourly and daily scatter plots to illustrate smoothing effect."""
    plt.figure(figsize=(8,6))
    plt.scatter(df_hourly["pm25_aqs"], df_hourly["pm25_purpleair"], alpha=0.25, label="Hourly", color="gray")
    plt.scatter(df_daily["pm25_aqs"], df_daily["pm25_purpleair"], alpha=0.8, label="Daily", color="darkorange")
    plt.plot([0, max(df_hourly["pm25_aqs"].max(), df_hourly["pm25_purpleair"].max())],
             [0, max(df_hourly["pm25_aqs"].max(), df_hourly["pm25_purpleair"].max())], 'k--', lw=1)
    plt.xlabel("AQS PM₂.₅ (µg/m³)")
    plt.ylabel("PurpleAir PM₂.₅ (µg/m³)")
    plt.title("Hourly vs Daily Comparison of AQS and PurpleAir")
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_bias_vs_humidity(df):
    """Plots mean and variability of bias (PurpleAir - AQS) by humidity."""
    df = df.dropna(subset=["pm25_aqs", "pm25_purpleair", "humidity"]).copy()
    df["bias"] = df["pm25_purpleair"] - df["pm25_aqs"]
    df["humidity_bin"] = pd.cut(df["humidity"], bins=np.arange(0, 110, 10))
    stats = df.groupby("humidity_bin")["bias"].agg(["mean", "std"]).reset_index()

    fig, ax1 = plt.subplots(figsize=(8,5))
    ax2 = ax1.twinx()
    sns.barplot(x="humidity_bin", y="mean", data=stats, ax=ax1, color="skyblue", label="Mean Bias")
    sns.lineplot(x="humidity_bin", y="std", data=stats, ax=ax2, color="red", label="Std Dev", marker="o")

    ax1.set_xlabel("Humidity (%)")
    ax1.set_ylabel("Mean Bias (PurpleAir - AQS)")
    ax2.set_ylabel("Bias Std Dev")
    ax1.set_title("Bias Behavior Across Humidity Levels")
    ax1.tick_params(axis='x', rotation=45)
    plt.tight_layout()
    plt.show()



def plot_bias_by_humidity_and_wind(df):
    """2D heatmap of mean bias across humidity/wind bins."""
    df = df.dropna(subset=["pm25_aqs", "pm25_purpleair", "humidity", "wind_speed_10m"]).copy()
    df["bias"] = df["pm25_purpleair"] - df["pm25_aqs"]
    df["humidity_bin"] = pd.cut(df["humidity"], bins=np.arange(0, 110, 10))
    df["wind_bin"] = pd.cut(df["wind_speed_10m"], bins=np.arange(0, df["wind_speed_10m"].max()+2, 2))
    pivot = df.pivot_table(values="bias", index="humidity_bin", columns="wind_bin", aggfunc="mean")

    plt.figure(figsize=(8,6))
    sns.heatmap(pivot, cmap="coolwarm", center=0, cbar_kws={"label": "Mean Bias (µg/m³)"})
    plt.title("Mean Bias by Humidity and Wind Speed")
    plt.xlabel("Wind Speed (m/s)")
    plt.ylabel("Humidity (%)")
    plt.tight_layout()
    plt.show()


def plot_lag_correlation(lags, correlations):
    """Line plot showing correlation vs time lag."""
    plt.figure(figsize=(7,4))
    plt.plot(lags, correlations, marker='o', color='teal')
    plt.axvline(0, color='gray', linestyle='--')
    plt.xlabel("Lag (hours)")
    plt.ylabel("Correlation (r)")
    plt.title("Lagged Correlation Between AQS and PurpleAir")
    plt.tight_layout()
    plt.show()


def plot_actual_vs_predicted(y_true, y_pred, title="Model Prediction vs AQS"):
    """Scatter of predicted vs actual AQS PM2.5."""
    plt.figure(figsize=(6,6))
    plt.scatter(y_true, y_pred, alpha=0.3, color="green")
    lim = [0, max(y_true.max(), y_pred.max())]
    plt.plot(lim, lim, 'k--', lw=1)
    plt.xlabel("Actual AQS PM₂.₅")
    plt.ylabel("Predicted PM₂.₅")
    plt.title(title)
    plt.tight_layout()
    plt.show()
  


# ---------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------
if __name__ == "__main__":
    print("📊 Generating result figures...")
    plot_model_comparison(df)
    plot_r2_highlight(df)
    plot_predicted_vs_observed()
    plot_all_model_scatters()
    save_performance_table_image(df)
    print(f"✅ Figures saved to: {FIGURES_DIR}")
