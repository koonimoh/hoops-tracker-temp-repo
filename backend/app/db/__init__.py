```python
from .supabase import supabase, supabase_client
from .schemas import *
from .search import search_service

__all__ = [
    'supabase', 
    'supabase_client', 
    'search_service',
]
```