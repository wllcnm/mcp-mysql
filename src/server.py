import asyncio
import logging
import os
from typing import List
import mysql.connector
from mcp.server import Server
from mcp.types import Tool, TextContent
from mcp.server.stdio import stdio_server
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mysql_mcp_server")

class MySQLMCPServer:
    def __init__(self):
        self.app = Server("mysql_mcp_server")
        self.setup_tools()

    def get_mysql_connection(self):
        load_dotenv()
        return mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE")
        )

    def setup_tools(self):
        @self.app.list_tools()
        async def list_tools() -> List[Tool]:
            return [
                Tool(
                    name="execute_query",
                    description="Execute a SQL query",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to execute"
                            }
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="list_tables",
                    description="List all tables in the database",
                    inputSchema={
                        "type": "object",
                        "properties": {}
                    }
                ),
                Tool(
                    name="describe_table",
                    description="Get table structure",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "Name of the table"
                            }
                        },
                        "required": ["table_name"]
                    }
                )
            ]

        @self.app.call_tool()
        async def call_tool(name: str, arguments: dict) -> List[TextContent]:
            conn = self.get_mysql_connection()
            cursor = conn.cursor(dictionary=True)
            
            try:
                if name == "execute_query":
                    query = arguments["query"]
                    try:
                        cursor.execute(query)
                        if query.strip().upper().startswith("SELECT"):
                            results = cursor.fetchall()
                            if not results:
                                return [TextContent(type="text", text="Query executed successfully. No results returned.")]
                            return [TextContent(type="text", text=str(results))]
                        else:
                            conn.commit()
                            return [TextContent(type="text", text=f"Query executed successfully. Affected rows: {cursor.rowcount}")]
                    except Exception as e:
                        return [TextContent(type="text", text=f"Error executing query: {str(e)}")]
                
                elif name == "list_tables":
                    try:
                        cursor.execute("SHOW TABLES")
                        tables = cursor.fetchall()
                        table_list = [list(table.values())[0] for table in tables]
                        return [TextContent(type="text", text=f"Tables in database:\n{', '.join(table_list)}")]
                    except Exception as e:
                        return [TextContent(type="text", text=f"Error listing tables: {str(e)}")]
                
                elif name == "describe_table":
                    table_name = arguments["table_name"]
                    try:
                        cursor.execute(f"DESCRIBE {table_name}")
                        columns = cursor.fetchall()
                        description = [f"Table structure for {table_name}:\n"]
                        for col in columns:
                            description.append(
                                f"Field: {col['Field']}, "
                                f"Type: {col['Type']}, "
                                f"Null: {col['Null']}, "
                                f"Key: {col['Key']}, "
                                f"Default: {col['Default']}, "
                                f"Extra: {col['Extra']}"
                            )
                        return [TextContent(type="text", text="\n".join(description))]
                    except Exception as e:
                        return [TextContent(type="text", text=f"Error describing table: {str(e)}")]
                
                else:
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]
            
            finally:
                cursor.close()
                conn.close()

    async def run(self):
        logger.info("Starting MySQL MCP server...")
        
        async with stdio_server() as (read_stream, write_stream):
            try:
                await self.app.run(
                    read_stream,
                    write_stream,
                    self.app.create_initialization_options()
                )
            except Exception as e:
                logger.error(f"Server error: {str(e)}", exc_info=True)
                raise

def main():
    server = MySQLMCPServer()
    asyncio.run(server.run())

if __name__ == "__main__":
    main() 