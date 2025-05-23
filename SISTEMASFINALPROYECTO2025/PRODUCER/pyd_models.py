
from datetime import datetime
from typing import List
from datetime import datetime
from typing import List, Optional

from pydantic import (
    BaseModel,
    validator,
)


class PredsModel(BaseModel):
    image_frame: str
    prob: float
    tags: List[str]

    def __getitem__(self, item):
        return getattr(self, item)


class DataModel(BaseModel):
    license_id: str
    preds: List[PredsModel]

    def __getitem__(self, item):
        return getattr(self, item)

class PayloadModel(BaseModel):
    id: str
    customer: str
    amount: float
    description: str
    status: Optional[str] = "Enviado"  # NUEVO CAMPO AÑADIDO con valor por defecto