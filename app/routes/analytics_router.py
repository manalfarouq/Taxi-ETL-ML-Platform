from fastapi import APIRouter, HTTPException
import psycopg2.extras
from app.db.db_connection import get_db_connection

router = APIRouter(prefix="/analytics", tags=["Analytics"])


def execute_query(query, params=None, one=False):
    """Helper function pour exécuter les requêtes SQL"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, params)
        result = cursor.fetchone() if one else cursor.fetchall()
        
        # Convertir en liste de dicts
        if one:
            return dict(result) if result else None
        else:
            return [dict(row) for row in result] if result else []
            
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Erreur SQL: {str(e)}"
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@router.get("/avg-duration-by-hour")
def avg_duration_by_hour():
    """Durée moyenne par heure de la journée"""
    query = """
    SELECT 
        EXTRACT(HOUR FROM tpep_pickup_datetime)::INT AS pickuphour,
        ROUND(AVG(trip_duration)::NUMERIC, 2) AS avg_duration,
        COUNT(*) AS total_trips
    FROM silver_trips
    WHERE trip_duration BETWEEN 1 AND 120
    GROUP BY pickuphour
    ORDER BY pickuphour;
    """
    return execute_query(query)


@router.get("/payment-analysis")
def payment_analysis():
    """Analyse par type de paiement"""
    query = """
    SELECT 
        payment_type,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_duration)::NUMERIC, 2) AS avg_duration,
        ROUND(MIN(trip_duration)::NUMERIC, 2) AS min_duration,
        ROUND(MAX(trip_duration)::NUMERIC, 2) AS max_duration
    FROM silver_trips
    WHERE trip_duration BETWEEN 1 AND 120
      AND payment_type IN (1,2,3,4)
    GROUP BY payment_type
    ORDER BY payment_type;
    """
    return execute_query(query)


@router.get("/top-routes")
def top_routes(limit: int = 10):
    """Top routes les plus empruntées"""
    if limit > 50:
        raise HTTPException(status_code=400, detail="Limit max: 50")
    
    query = """
    SELECT 
        pulocationid,
        dolocationid,
        COUNT(*) AS trip_count,
        ROUND(AVG(trip_duration)::NUMERIC, 2) AS avg_duration,
        ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance
    FROM silver_trips
    WHERE trip_duration BETWEEN 1 AND 120
      AND trip_distance > 0.1
    GROUP BY pulocationid, dolocationid
    ORDER BY trip_count DESC
    LIMIT %s;
    """
    return execute_query(query, (limit,))


@router.get("/weekly-patterns")
def weekly_patterns():
    """Patterns par jour de la semaine"""
    query = """
    WITH daily_stats AS (
        SELECT 
            EXTRACT(DOW FROM tpep_pickup_datetime)::INT AS day_of_week,
            trip_duration,
            CASE 
                WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN 'Weekend'
                ELSE 'Weekday'
            END AS day_type
        FROM silver_trips
        WHERE trip_duration BETWEEN 1 AND 120
    )
    SELECT 
        day_of_week,
        day_type,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_duration)::NUMERIC, 2) AS avg_duration
    FROM daily_stats
    GROUP BY day_of_week, day_type
    ORDER BY day_of_week;
    """
    return execute_query(query)


@router.get("/model-performance")
def model_performance():
    """Statistiques de performance du modèle"""
    query = """
    SELECT 
        COUNT(*) AS total_predictions,
        ROUND(AVG(predicted_duration)::NUMERIC, 2) AS avg_predicted_duration,
        ROUND(MIN(predicted_duration)::NUMERIC, 2) AS min_predicted_duration,
        ROUND(MAX(predicted_duration)::NUMERIC, 2) AS max_predicted_duration,
        model_version,
        DATE(MIN(timestamp)) AS first_prediction,
        DATE(MAX(timestamp)) AS last_prediction
    FROM eta_predictions
    GROUP BY model_version;
    """
    result = execute_query(query, one=True)
    return result if result else {"message": "Aucune prédiction enregistrée"}