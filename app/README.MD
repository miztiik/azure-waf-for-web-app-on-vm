# Azure Python Function Local Environment Setup

```bash
FUNC_DIR="function_code"
FUNC_PROJ_NAME="store-backend-ops"
FUNC_APP_NAME="store-backend-fnApp-006"
FUNC_NAME="store-events-consumer-fn"

cd app/
mkdir -p ${FUNC_DIR}
cd ${FUNC_DIR}
python -m venv .venv
source .venv/Scripts/activate

func init ${FUNC_PROJ_NAME} --python -m V1
cd ${FUNC_PROJ_NAME}

# Create function from template
func new --name ${FUNC_NAME} --template "Azure Event Grid trigger"

# Publishing the function to Azure Functions
# func azure functionapp publish ${FUNC_APP_NAME} --nozip
```