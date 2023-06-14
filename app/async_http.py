import logging
import asyncio
import datetime
import json
import azure.functions as func

async def sqr_num(count: int) -> int:
    result = count * count
    return result

async def validate_input(value):
    try:
        num = int(value)
    except (ValueError, TypeError):
        num = 0
    return num

async def main(req: func.HttpRequest) -> func.HttpResponse:
    _resp = {
        "status": False,
        "recv_num": ""
    }
    num = req.params.get("count")
    await asyncio.sleep(2)
    num = await validate_input(num)

    if num == 0:
        _resp["msg"] = "Invalid input, or you chose it"

    _resp["recv_num"] = num
    num_sqr = await sqr_num(int(num))
    logging.info(f"num_sqr: {num_sqr}")
    _resp["status"] = True
    _resp["num_sqr"] = num_sqr
    _resp["processed_on"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return func.HttpResponse(json.dumps(_resp), status_code=200)

