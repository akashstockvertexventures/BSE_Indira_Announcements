import asyncio
import json
import aiohttp
from datetime import datetime
from core.base import Base
from openai import AsyncOpenAI
from utils.symbolmap_updator import AsyncSymbolMapUpdater
from config.settings import SQUACK_URL, OPENAI_API_KEY, OPENAI_MODEL

class LivesquackNewsLLM(Base):
    def __init__(self):
        super().__init__("livesquack_news_llm")
        self.input_tokens = 0
        self.output_tokens = 0
        self.counter = 0
        self.process_type = "nrml"
        self.squack_url = SQUACK_URL
        self.chatgpt_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.chatgpt_model = OPENAI_MODEL
        self.symbol_map_updator = AsyncSymbolMapUpdater()
        # self.logger = logging.getLogger(__name__)
        # logging.basicConfig(level=logging.INFO)

    def get_schema(self, nse): 
        json_schema_news_impact = {
            "format": {
                "type": "json_schema",
                "name": "news_impact_analysis",
                "schema": {
                    "type": "object",
                    "properties": {
                        "impact": {
                            "type": "string",
                            "description": "Description of the impact of the news. Keep it crisp and 10-20 words"
                        },
                        "impact_score": {
                            "type": "number",
                            "description": "Strength of the news impact on a scale from 0 (no impact) to 10 (high impact)."
                        },
                        
                        "category": {
                            "type": "string",
                            "enum": ["Financial Results", "Earnings Call Transcript", "Investor Presentation", "Amalgamation / Merger / Demerger", "Acquisition", "Order Wins", "Capacity Expansion or New Ventures", "Management / Board", "Corporate Action", "Legal / Regulatory","Analyst / Broker Target","NSE Block / Bulk Deal", "General / Other"],
                            "description": "Category of the news or announcement. Choices:\n\
                        - 'Financial Results': Periodic financial disclosures such as quarterly/annual results, board meeting outcomes on results, half-yearly reports, or statutory filings about financials.\n\
                        - 'Earnings Call Transcript': Summaries, transcripts, or announcements of upcoming or completed company earnings calls, including Q&A, performance, and outlook. Use this category even for advance notices or summaries of such calls.\n\
                        - 'Investor Presentation': Presentations, materials, or announcements for investors, including analyst meets, investor decks, business updates communicated via slides, or notifications about upcoming or recently held investor presentations.\n\
                        - 'Amalgamation / Merger / Demerger': Company restructuring events like mergers, demergers, amalgamations, court meetings, or arrangements.\n\
                        - 'Acquisition': News about acquisition or planned acquisition of companies/assets, including major share purchases or agreements related to M&A.\n\
                        - 'Order Wins': Announcements regarding significant new contracts, tenders, project wins, large purchase orders, or other business awards received by the company. Includes subcategories such as 'Award of Order / Receipt of Order', 'Order Wins', 'Joint Ventures or Collaborations' (when focused on project wins), and similar disclosures.\n\
                        - 'Capacity Expansion or New Ventures': Announcements regarding expansions of production capacity, new facilities, major capital expenditures, or launches of new business ventures, products, or services. \n\
                        - 'Management / Board': Updates on leadership, board, or management changes—appointments, resignations, changes in control, or related meetings.\n\
                        - 'Corporate Action': Actions impacting shareholders or capital structure—dividends, bonuses, splits, buybacks, delisting, rights issues, preferential allotments, and open offers.\n\
                        - 'Analyst / Broker Target':Whenever any broker or an analyst provides an upgrade downgrade on a stock\n\
                        - 'Legal / Regulatory': News about legal proceedings, litigation, court or tribunal orders, regulatory actions (SEBI/RBI/stock exchanges), insolvency/bankruptcy cases, compliance filings, or statutory legal certificates.\n\
                        - 'NSE Block / Bulk Deal': Disclosures of large trades conducted through the National Stock Exchange (NSE) block or bulk deal window. Use this category only if the news specifically mentions NSE block or bulk deals (do not use for BSE or generic block/bulk deals).\n\
                        - 'General / Other': Any other news, disclosures, or announcements not covered above—press releases, ESG, incidents, business updates, or miscellaneous."
                        },

                        "sentiment": {
                            "type": "string",
                            "enum": ["Positive", "Negative", "Neutral"],
                            "description": "Overall sentiment implied by the news for the stock/market."
                        }
                    },
                    "required": ["impact", "impact_score", "category", "sentiment"],
                    "additionalProperties": False
                },
                "strict": True
            }
        }

        if not nse:
            json_schema_news_impact["format"]["schema"]["properties"]["nse_symbols_list"] = {
                "type": "array",
                "description": (
                    "List of NSE symbols that can be impacted by this news. "
                    "Leave as an empty list if market-wide/general news "
                    "If it's sector-wide news, list the stocks that can be majorly impacted within that sector."
                    "If custome name is given focus on that to identify nse symbol based on custom name"
                    "if news description have words like stocks to watch or description have multiple stocks name then nse_symbols_list key strictly empty"
                ),
                "items": {"type": "string"}
            }
            json_schema_news_impact["format"]["schema"]["required"].append("nse_symbols_list")
        return json_schema_news_impact

    async def usage_updator(self, usage_dict):
        try:
            datecode = datetime.now().strftime("%Y%m%d")
            total_cost = ((usage_dict["input_tokens"] + 4 * usage_dict["output_tokens"]) * float(usage_dict["model_cost"])/1000000)
            filter_query = {
                "datecode": datecode,
                "LiveSquackNews": {
                    "$elemMatch": {
                        "model": usage_dict["model"],
                        "process_type": usage_dict["process_type"]
                    }
                }
            }
            update_query = {
                "$inc": {
                    "LiveSquackNews.$.input_tokens": usage_dict["input_tokens"],
                    "LiveSquackNews.$.output_tokens": usage_dict["output_tokens"],
                    "LiveSquackNews.$.request_count": usage_dict["request_count"],
                    "LiveSquackNews.$.Total_cost": total_cost
                }
            }
            result = await self.llm_usage_collection.update_one(
                filter_query,
                update_query,
                upsert=False
            )
            if result.matched_count == 0:
                filter_query = {"datecode": datecode}
                update_query = {
                    "$push": {
                        "LiveSquackNews": {
                            "model": usage_dict["model"],
                            "process_type": usage_dict["process_type"],
                            "input_tokens": usage_dict["input_tokens"],
                            "output_tokens": usage_dict["output_tokens"],
                            "request_count": usage_dict["request_count"],
                            "Total_cost": total_cost
                        }
                    }
                }
                await self.llm_usage_collection.update_one(
                    filter_query,
                    update_query,
                    upsert=True
                )
            self.logger.info(f"Usage updated for model: {usage_dict['model']}, process: {usage_dict['process_type']}, datecode: {datecode}")
        except Exception as e:
            self.logger.error(f"Failed to update usage: {str(e)}")
            raise

    async def cost_calculation(self, input, output, model):
        rates = {
            "gpt-4.1": (2, 8),
            "gpt-4.1-mini": (0.4, 1.6),
            "gpt-4.1-nano": (0.10, 0.4),
            "gpt-4o": (2.5, 10),
        }
        input_rate, output_rate = rates.get(model, (0.15, 0.6)) 

        self.input_tokens += input
        self.output_tokens += output
        self.counter += 1

        input_cost = (self.input_tokens / 1_000_000) * input_rate
        output_cost = (self.output_tokens / 1_000_000) * output_rate
        cost = input_cost + output_cost

        self.logger.info(
            f"input_tokens = {input}, output_tokens = {output}, "
            f"total_input = {self.input_tokens}, total_output = {self.output_tokens}\n"
            f"input_cost = {input_cost:.6f}, output_cost = {output_cost:.6f}, "
            f"total_cost = {cost:.6f}\nRequest_count = {self.counter}"
        )
        usage_dict = {"input_tokens":input, "output_tokens":output,"model":model,"model_cost":input_rate,"process_type":self.process_type,"request_count":1}
        await self.usage_updator(usage_dict)
        return cost

    async def get_impact_score_sentiment(self, text, custom_name, schema, get_symbol_from_llm):

        if get_symbol_from_llm:
            prompt = f"""
            custom_name: {custom_name}, news description: {text}
            Analyze the following news description and custom name to determine its impact on the Indian stock market.
            - Perform a broader market analysis to assess the news' impact.
            - Identify the NSE stock symbol(s) most likely affected by this news. Use an empty list [] if the news pertains to the overall market or no specific stocks are impacted.
            - For sector-wide news, list all major NSE symbols likely affected as a list.
            - Provide a brief reasoning for the predicted impact and stock selection.
            - If custome name is given focus on that to identify nse symbol based on custom name
            Return the answer in strict given JSON format.
            """
            
        else:
            prompt = f"""
                custom_name: {custom_name}, news description: {text}
                Analyze the following news description and custom name to determine its impact on the Indian stock market.
                - Perform a broader market analysis to assess the news' impact.
                - Provide a brief reasoning for the predicted impact.
                Return the answer in strict given JSON format.
            """

        try:
            response = await self.chatgpt_client.responses.create(
                model=self.chatgpt_model,
                input=[
                    {"role": "system", "content": "You are a news analyst"},
                    {"role": "user", "content": prompt},
                ],
                text=schema
            )
            content = response.output_text
            await self.cost_calculation(response.usage.input_tokens, response.usage.output_tokens, self.chatgpt_model)
            return json.loads(content)
        except Exception as e:
            self.logger.error(f"LLM parse error: {e}")
            return {}  
        
    async def fetch_and_process_news(self, session, get_symbol_from_llm=True):
        try:
            async with session.get(self.squack_url) as resp:
                data = await resp.json()

            to_insert = []
            insertdocs = []
            insertdocs_withoutSymbol = []
            if data:
                for item in data.get("items", []):
                    news_id = item.get("guid")
                    date = item.get("date")
                    try:
                        dt = datetime.strptime(date, "%a %B %d %Y %H:%M:%S")
                    except ValueError:
                        self.logger.error(f"Invalid date format for news_id {news_id}: {date}")
                        continue
                    dt_tm = dt.strftime("%Y-%m-%d %H:%M:%S")
                    description = item.get("description", "")
                    custom_elements = item.get("custom_elements", [])
                    nse_symbol = ""
                    custom_name = ""
                    if custom_elements:
                        companies = custom_elements[0].get("companies", [])
                        custom_name = custom_elements[0].get("customName", "")
                        if companies:
                            nse_symbol = companies[0].get("nse", "")

                    exists = await self.collection_squack.find_one({"news_id": news_id})
                    if not exists:
                        to_insert.append({
                            "news_id": news_id,
                            "dt_tm": dt_tm,
                            "nse_symbol": nse_symbol,
                            "custom_name": custom_name,
                            "short_summary": description
                        })
                if to_insert:
                    self.logger.info(f"Found {len(to_insert)} new news items to process.")
                    for row in to_insert:
                        self.logger.info(f"start llm processing new news id: {row['news_id']}")
                        if row['nse_symbol'] != "":
                            schema = self.get_schema(True)
                            result = await self.get_impact_score_sentiment(row["short_summary"], row["custom_name"], schema, get_symbol_from_llm)
                            if not result:
                                continue
                            insertdocs.append({
                                "news_id": row.get("news_id"),
                                "dt_tm": row.get("dt_tm"),
                                "nse_symbol": row.get('nse_symbol'),
                                "custom_name": row.get("custom_name"),
                                "short summary": row.get("short_summary"),
                                "impact": result.get("impact"),
                                "impact score": result.get("impact_score"),
                                "sentiment": result.get("sentiment"),
                                "category": result.get("category"),
                                "created_at": datetime.now()
                            })

                        elif row['nse_symbol'] == "":
                            if not get_symbol_from_llm:
                                schema = self.get_schema(True)
                                result = await self.get_impact_score_sentiment(row["short_summary"], row["custom_name"], schema, get_symbol_from_llm)
                                if not result:
                                    continue
                                insertdocs_withoutSymbol.append({
                                    "news_id": row.get("news_id"),
                                    "dt_tm": row.get("dt_tm"),
                                    "nse_symbol": "",
                                    "custom_name": row.get("custom_name"),
                                    "short summary": row.get("short_summary"),
                                    "impact": result.get("impact"),
                                    "impact score": result.get("impact_score"),
                                    "sentiment": result.get("sentiment"),
                                    "category": result.get("category"),
                                    "created_at": datetime.now()
                                })

                            else:
                                schema = self.get_schema(False)
                                result = await self.get_impact_score_sentiment(row["short_summary"], row["custom_name"], schema, get_symbol_from_llm)
                                if not result:
                                    self.logger.info("No LLm result found")
                                    continue
                                nse_symbols = result.get('nse_symbols_list', [])
                                if not nse_symbols:
                                    self.logger.info("No nse symbol list found")
                                    insertdocs_withoutSymbol.append({
                                        "news_id": row.get("news_id"),
                                        "dt_tm": row.get("dt_tm"),
                                        "nse_symbol": "",
                                        "custom_name": row.get("custom_name"),
                                        "short summary": row.get("short_summary"),
                                        "impact": result.get("impact"),
                                        "impact score": result.get("impact_score"),
                                        "sentiment": result.get("sentiment"),
                                        "category": result.get("category"),
                                        "created_at": datetime.now()
                                    })
                                    continue

                                elif nse_symbols:
                                    insertdocs.extend([
                                        {
                                            "news_id": row.get("news_id"),
                                            "dt_tm": row.get("dt_tm"),
                                            "nse_symbol": nse,
                                            "custom_name": row.get("custom_name"),
                                            "short summary": row.get("short_summary"),
                                            "impact": result.get("impact"),
                                            "impact score": result.get("impact_score"),
                                            "sentiment": result.get("sentiment"),
                                            "category": result.get("category"),
                                            "created_at": datetime.now()
                                        } for nse in nse_symbols
                                    ])

                    if insertdocs_withoutSymbol:
                        await self.collection_squack.insert_many(insertdocs_withoutSymbol)
                        self.logger.info(f"Inserted news_ids without symbols {[doc['news_id'] for doc in insertdocs_withoutSymbol]}, No of docs inserted: {len(insertdocs_withoutSymbol)}")

                    if insertdocs:
                        updated_docs = await self.symbol_map_updator.update_input_data_livesquack(insertdocs)
                        if updated_docs:
                            await self.collection_squack.insert_many(updated_docs)
                        self.logger.info(f"Inserted news_ids with symbols {[doc['news_id'] for doc in updated_docs]}, No of docs after symbolmap updator, {len(updated_docs)} out of {len(insertdocs)}")

                else:
                    self.logger.info("Found 0 new news items to process.")
            
            else:
                self.logger.info("No data found after fetch from url")

        except Exception as ex:
            self.logger.error(f"Error in processing news: {ex}")
            await asyncio.sleep(60)

    async def run(self, interval=60):
        try:
            self.logger.info("Running fetch and process news method")
            async with aiohttp.ClientSession() as session:
                while True:
                    await self.fetch_and_process_news(session=session, get_symbol_from_llm=False)
                    await asyncio.sleep(interval)
        except Exception as ex:
            self.logger.error(f"Run error: {ex}")


