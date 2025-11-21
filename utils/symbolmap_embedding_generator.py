import aiohttp
import asyncio
from typing import List, Dict
from datetime import datetime
from core.base import Base
from sentence_transformers import SentenceTransformer

class AsyncSymbolMapBuilder(Base):
    
    def __init__(self, api_url: str = "https://admin.stocksemoji.com/api/indira-cmots/CompanyMaster", model_name: str = 'all-MiniLM-L6-v2'):
        super().__init__()
        self.model_name = model_name
        self.api_url = api_url
        self.entries: List[Dict] = []

    async def fetch_company_master(self) -> List[Dict]:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.api_url) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data.get('data', [])

    def should_exclude(self, name: str) -> bool:
        name = name.lower().replace(" ", "")
        return "partlypaidup" in name

    def build_symbol_map(self, company: Dict) -> Dict:
        nse = company.get("nsesymbol", "").strip().upper()
        bse = company.get("bsecode", None)
        name = company.get("companyname", "").strip()
        isin = company.get("isin", "")
        selected = nse if nse else int(bse) if bse else None
        return {
            "company":isin,
            "NSE": nse,
            "BSE": int(bse) if bse else None,
            "Company_Name": name,
            "SELECTED": selected
        }

    def build_embedding_text(self, symbol_map: Dict) -> str:
        return f"{symbol_map['Company_Name']} {symbol_map['NSE']}"

    async def generate(self):
        companies = await self.fetch_company_master()
        texts_company_and_symbol = []
        texts_symbol_only = []
        symbol_maps = []

        for comp in companies:
            name = comp.get("companyname", "").strip()
            if self.should_exclude(name):
                continue 
            sm = self.build_symbol_map(comp)
            text_combined = self.build_embedding_text(sm)
            text_nse_only = sm["NSE"]
            symbol_maps.append(sm)
            texts_company_and_symbol.append(text_combined)
            texts_symbol_only.append(text_nse_only)

        embedding_company_and_symbol = self.model.encode(texts_company_and_symbol, convert_to_tensor=False)
        embedding_symbol_only = self.model.encode(texts_symbol_only, convert_to_tensor=False)

        self.entries = [
            {
                "symbol_map": sm,
                "embedding_company_and_symbol": emb1.tolist() if hasattr(emb1, 'tolist') else emb1,
                "embedding_symbol_only": emb2.tolist() if hasattr(emb2, 'tolist') else emb2,
            }
            for sm, emb1, emb2 in zip(symbol_maps, embedding_company_and_symbol, embedding_symbol_only)
        ]

        if self.entries:
            await self.collection_symbolmap_embeddings.delete_many({})
            await self.collection_symbolmap_embeddings.insert_many(self.entries)
            await self.collection_metadata_updates.update_one({}, {"$set": {"Symbolmap Embeddings": datetime.now()}})
            print(f"Saved {len(self.entries)} entries of symbol map embeddings (excluding partly paid-up).")

    async def run(self):
        metadata = await self.collection_metadata_updates.find_one({"Symbolmap Embeddings": {"$exists": True}})
        if metadata:
            last_update = metadata.get("Symbolmap Embeddings")
            if last_update.date() == datetime.now().date():
                print("Symbol map Embeddings Already updated Today (excluding partly paid-up).")
                return  
        self.model = SentenceTransformer(self.model_name)
        await self.generate()

if __name__ == '__main__':
    async def main():
        builder = AsyncSymbolMapBuilder()
        await builder.run()
    asyncio.run(main())
