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
