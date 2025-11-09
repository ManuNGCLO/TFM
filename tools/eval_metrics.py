import argparse
import pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score
from sklearn.preprocessing import MultiLabelBinarizer

# Función para calcular precisión, recall y F1
def calculate_metrics(y_true, y_pred):
    # Usar MultiLabelBinarizer para convertir las listas en una representación binaria
    mlb = MultiLabelBinarizer()
    y_true_bin = mlb.fit_transform(y_true)
    y_pred_bin = mlb.transform(y_pred)
    
    # Calcular las métricas
    precision = precision_score(y_true_bin, y_pred_bin, average='macro', zero_division=0)
    recall = recall_score(y_true_bin, y_pred_bin, average='macro', zero_division=0)
    f1 = f1_score(y_true_bin, y_pred_bin, average='macro', zero_division=0)
    accuracy = accuracy_score(y_true_bin, y_pred_bin)
    
    return precision, recall, f1, accuracy

def main():
    parser = argparse.ArgumentParser(description="Evaluación de modelos con preguntas y respuestas")
    parser.add_argument("--truth", help="Archivo CSV con las preguntas y las respuestas correctas (ground truth)")
    parser.add_argument("--pred", help="Archivo CSV con las predicciones generadas por los modelos")
    parser.add_argument("--engines", nargs="+", help="Motores a evaluar (ej. rules, rules_fb, gpt, gpt_fb)", required=True)
    args = parser.parse_args()

    # Cargar preguntas y respuestas correctas (ground truth)
    ground_truth_df = pd.read_csv(args.truth)

    # Cargar predicciones de todos los modelos
    predictions_df = pd.read_csv(args.pred)

    # Asegúrate de que las columnas sean las correctas
    if 'question' not in ground_truth_df.columns or 'ground_truth' not in ground_truth_df.columns:
        raise ValueError("El archivo de preguntas debe tener las columnas 'question' y 'ground_truth'.")
    if 'question' not in predictions_df.columns:
        raise ValueError("El archivo de predicciones debe tener la columna 'question' y columnas para cada motor (por ejemplo, 'rules_prediction', 'rules_fb_prediction', etc.).")

    # Inicializar diccionario para almacenar las métricas por motor
    results = {}

    # Comparar las respuestas correctas con las respuestas predichas
    for engine in args.engines:
        # Nombre de la columna correspondiente al motor
        engine_col = f"{engine}_prediction"

        if engine_col not in predictions_df.columns:
            raise ValueError(f"El archivo de predicciones no contiene la columna '{engine_col}' para el motor {engine}.")

        # Inicializar listas para almacenar las respuestas correctas y las predicciones
        y_true = []
        y_pred = []

        for _, row in ground_truth_df.iterrows():
            question = row['question']
            correct_answer = row['ground_truth']

            # Buscar la predicción para la misma pregunta de cada motor
            prediction = predictions_df[predictions_df['question'] == question][engine_col].values[0]

            # Convertir las respuestas a listas de elementos (si son listas de términos, artículos, etc.)
            true_set = set(correct_answer.split(','))
            pred_set = set(prediction.split(','))

            # Comparar las respuestas
            y_true.append(list(true_set))
            y_pred.append(list(pred_set))

        # Calcular métricas para cada motor
        precision, recall, f1, accuracy = calculate_metrics(y_true, y_pred)
        results[engine] = {
            "Precision": precision,
            "Recall": recall,
            "F1": f1,
            "Accuracy": accuracy
        }

    # Mostrar los resultados por motor
    print("\n=== Resultados por Motor ===")
    for engine, metrics in results.items():
        print(f"\n{engine}:")
        print(f"  Precisión: {metrics['Precision']:.3f}")
        print(f"  Recall: {metrics['Recall']:.3f}")
        print(f"  F1: {metrics['F1']:.3f}")
        print(f"  Exactitud: {metrics['Accuracy']:.3f}")

if __name__ == "__main__":
    main()
