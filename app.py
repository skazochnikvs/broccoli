from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import date
import oracledb

from db import get_connection

app = FastAPI(title="Oracle Client API")

class DeptInfoRequest(BaseModel):
    dept_id: int

@app.get("/view/dept-goods-sales")
def view_dept_goods_sales(limit: int = 50):
    """
    1) Перегляд інформації з пов'язаних таблиць:
    Departments -> Goods -> Sales
    """
    sql = """
    SELECT d.dept_id, d.name AS dept_name,
           g.good_id, g.name AS good_name, g.price, g.quantity,
           s.sales_id, s.check_no, s.date_sale, s.quantity AS sold_qty
    FROM Departments d
    LEFT JOIN Goods g ON g.dept_id = d.dept_id
    LEFT JOIN Sales s ON s.good_id = g.good_id
    ORDER BY d.dept_id, g.good_id, s.sales_id
    FETCH FIRST :lim ROWS ONLY
    """
    try:
        with get_connection() as con:
            with con.cursor() as cur:
                cur.execute(sql, lim=limit)
                cols = [c[0].lower() for c in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                return {"count": len(rows), "rows": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/proc/update-dept-info")
def call_proc_update_dept_info(req: DeptInfoRequest):
    """
    2) Виклик збереженої процедури з параметром:
    update_dept_info(p_dept_id NUMBER)
    """
    try:
        with get_connection() as con:
            with con.cursor() as cur:
                cur.callproc("update_dept_info", [req.dept_id])
            con.commit()

        with get_connection() as con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT dept_id, name, info FROM Departments WHERE dept_id = :id",
                    id=req.dept_id
                )
                r = cur.fetchone()
                if not r:
                    return {"message": "Procedure executed, but dept not found", "dept_id": req.dept_id}
                return {"message": "Procedure executed", "dept": {"dept_id": r[0], "name": r[1], "info": r[2]}}
    except oracledb.DatabaseError as e:
        err = e.args[0]
        raise HTTPException(status_code=400, detail=f"Oracle error: {err.message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/func/goods-by-date")
def call_func_goods_by_date(d: date):
    """
    3) Виконання функції goods_by_date(p_date DATE) RETURN VARCHAR2
    """
    try:
        with get_connection() as con:
            with con.cursor() as cur:
                cur.execute("SELECT goods_by_date(:d) FROM dual", d=d)
                res = cur.fetchone()
                return {"date": str(d), "result": res[0] if res else None}
    except oracledb.DatabaseError as e:
        err = e.args[0]
        raise HTTPException(status_code=400, detail=f"Oracle error: {err.message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/test/exception")
def test_exception():
    """
    4) Обробка винятку, згенерованого сервером:
    CALL throw_test_error -> ловимо RAISE_APPLICATION_ERROR
    """
    try:
        with get_connection() as con:
            with con.cursor() as cur:
                cur.callproc("throw_test_error")
        return {"message": "No error (unexpected)"}
    except oracledb.DatabaseError as e:
        err = e.args[0]
        return {
            "handled": True,
            "oracle_code": getattr(err, "code", None),
            "oracle_message": getattr(err, "message", str(e))
        }
