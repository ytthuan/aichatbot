
import datetime
import os
from azure.storage.blob import generate_blob_sas, BlobSasPermissions, ContentSettings
import uuid
from bson import ObjectId

from typing import TYPE_CHECKING, Dict, List, Literal, TypedDict,Optional, Union
# version chainlit 1.3.0rc1  
from literalai.observability.step import StepDict as LiteralStepDict, Step, ScoreType, ScoreDict, StepType, AttachmentDict
from pymongo import DESCENDING, ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError
from pymongo import TEXT
from literalai import Thread
from literalai import Score as ClientFeedback
from pymongo import MongoClient
from chainlit.step import logger, Step, StepDict
from chainlit.types import ThreadFilter


from literalai import (
    Attachment,
    PaginatedResponse,
    UserDict,
    User
)

#NOTE: for all the vector db part, you can comment or remove that part
class MONGODB_API:

    #initialize the MongoDB client
    def __init__(self, mongodb_uri: str):
        # MongoDB client
        self.mongodb_client: MongoClient = MongoClient(mongodb_uri,connect=True)
        db = self.mongodb_client.get_database(os.getenv("MONGODB_NAME"))
        self.users = db["users"]
        self.threads = db["threads"]
        self.feedbacks = db["feedbacks"]
        self.attachments = db["attachments"]
        self.steps = db["steps"]
        self.starters = db["starters"]

        # Create indexes for faster querying
        try:
            self.users.create_index([("identifier", ASCENDING)], background=True)
            self.threads.create_index(
                [("createdAt", ASCENDING),("_id", ASCENDING)],
                background=True,
            )
            self.threads.create_index([("name", TEXT)], background=True)
            self.steps.create_index([("threadId", ASCENDING)], background=True)
            self.steps.create_index([("output.content", TEXT)], background=True)
            
            self.attachments.create_index([("threadId", ASCENDING), ("stepId", ASCENDING)], background=True)
            self.feedbacks.create_index([("threadId", ASCENDING), ("stepId", ASCENDING)], background=True)
            self.starters.create_index([("id", ASCENDING)], background=True)
            # self.generations.create_index([("createdAt", DESCENDING)], background=True)
        except DuplicateKeyError:
            logger.info("Indexes already exist!")
        except PyMongoError as e:
            logger.warning("Errors creating indexes MongoDB: %r", e)

        #logger.info("Mongo API initialized")


#region User API
    async def get_user_api(
        self, id: Optional[str] = None, identifier: Optional[str] = None
    ) -> Optional[User]:
        #logger.info("implemtation of get_user in Mongodb API for user")
        if id is None and identifier is None:
            raise Exception("Either id or identifier must be provided")
        if id is not None and identifier is not None:
            raise Exception("Only one of id or identifier must be provided")
        query = {}
        if id:
            query["id"] = id
        if identifier:
            query["identifier"] = identifier
        if query == {}:  # No query provided
            return None
        user = self.users.find_one(query)
        return User.from_dict(user) if user else None

    async def create_user_api(
        self, identifier: str, metadata: Optional[Dict] = None
    ) -> User:
        #logger.info("implemtation of create_user in Mongodb API: ", identifier)
        metadata = metadata or {}
        user = {
                "id":str(uuid.uuid4()),
                "identifier": identifier, 
                "metadata": metadata,
                "createdAt": datetime.datetime.now().isoformat()
                }
        
        
        oid = self.users.insert_one(user).inserted_id
        if not oid:
            return None
        #logger.info("User created: %r", user["identifier"])
        _user = self.users.find_one({"_id": ObjectId(oid)})
        return User.from_dict(_user)
    
    async def update_user_api(
        self, id: str, identifier: Optional[str] = None, metadata: Optional[Dict] = None
    ) -> User:
        user = {"identifier": identifier, "metadata": metadata}
        metadata = metadata or {}
        # remove None values to prevent the API from removing existing values
        _user = {k: v for k, v in user.items() if v is not None}

        result = self.users.find_one_and_update({"id": id}, {"$set":_user},return_document=ReturnDocument.AFTER, upsert=True)
        #logger.info("User updated: %r", result["id"])
        
        return User.from_dict(result)

#endregion
    
#region Feedback API
    async def create_feedback_api(
        self,
        name: str,
        value: int,
        type: ScoreType,
        step_id: Optional[str] = None,
        generation_id: Optional[str] = None,
        dataset_experiment_item_id: Optional[str] = None,
        comment: Optional[str] = None,
        tags: Optional[List[str]] = None,
        
    ) -> Optional[ClientFeedback]:
        """
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "value": self.value,
            "stepId": self.step_id,
            "generationId": self.generation_id,
            "datasetExperimentItemId": self.dataset_experiment_item_id,
            "comment": self.comment,
            "tags": self.tags,
        """
        if not step_id:
            return None

        thread_id = self.steps.find_one({"id": step_id}).get("threadId")
        if not thread_id:
            raise ValueError("Thread not found")
       
        _feedback = {
            "id": str(uuid.uuid4()),
            "name": name,
            "type": type,
            "value": value,
            "stepId": step_id,
            "threadId": thread_id,
            "generationId": generation_id,
            "datasetExperimentItemId": dataset_experiment_item_id,
            "comment": comment,
            "tags": tags }

        result = self.feedbacks.insert_one(_feedback).inserted_id
        if not result:
            return None
        feedback = self.feedbacks.find_one({"_id": ObjectId(result)},{ "_id": 0})

        return ClientFeedback.from_dict(feedback)


    class FeedbackUpdate(TypedDict, total=False):
        comment: Optional[str]
        value: Optional[int]

    async def update_feedback_api(
        self,
        id: str,  
        # stepd_id: str,
        update_params: FeedbackUpdate
    ) -> "ClientFeedback":
        """
        
             {
                id
                stepId
                value
                comment
            }
            
        """
        
        _feedback = {}
        _feedback["comment"] = update_params.get("comment")
        _feedback["value"] = update_params.get("value") 
  
        result = self.feedbacks.find_one_and_update({"id": id},{"$set":  _feedback},return_document=ReturnDocument.AFTER)
        if not result:
            raise ValueError("Feedback not found")
        
        return ClientFeedback.from_dict(result)
    
    async def delete_feedback_api(self, id: str) -> bool:
        logger.info("Deleting feedback: %r", id)
        result = self.feedbacks.delete_one({"id": id}).deleted_count
        return result > 0
    
#endregion 
    
#region Element API
    
    async def create_attachment_api(
        self, 
        metadata: Optional[Dict] = None,
        mime: Optional[str] = None,
        name: Optional[str] = None,
        object_key: Optional[str] = None,
        step_id: Optional[str] = None,
        thread_id: Optional[str] = None,    
        url: Optional[str] = None,
        id: Optional[str] = None
    ) -> "Attachment":
        
        """ 
        creating parms:{
                id
                threadId
                stepId
                metadata
                mime
                name
                objectKey
                url
            }
        """
        if not metadata:
            metadata = {"size": None, "language": None, "page": None, "page": None}
        metadata["display"] = "inline"

        if mime == "text/plain":
            metadata["type"] = "text"
            metadata["display"] = "side"
        elif mime == "image/jpeg":
            metadata["type"] = "image"
        elif mime == "application/pdf":
            metadata["type"] = "pdf"
        elif mime == "audio/mpeg":
            metadata["type"] = "audio"
        elif mime == "video/mp4":
            metadata["type"] = "video"
        _attachment = {
            "metadata": metadata,
            "mime": mime,
            "name": name,
            "objectKey": object_key,
            "stepId": step_id,
            "threadId": thread_id,
            "url": url,
            "id": id,
        }
        result =  self.attachments.insert_one(_attachment).inserted_id
        if not result:
            return None
        attachment =  self.attachments.find_one({"_id": ObjectId(result)})
        return Attachment.from_dict(attachment)
    
    async def get_attachment_api(self, id: str) -> Optional[Attachment]:
        """
         return data example:
            {
                id
                threadId
                stepId
                metadata
                mime
                name
                objectKey
                url
            }
        
        """
        variables = {"id": id}
        result =  self.attachments.find_one(variables)
        return Attachment.from_dict(result) if result else None

    
    async def delete_attachments_api(self, threadId: str) -> bool:
        """
        delete many attachments
        delete parms: {threadId} - thread id
        """
        variables = {"threadId": threadId}
        object_list = []
        _attachment =  self.attachments.find(variables)
        if not _attachment:
            return True
        for attachment in _attachment:
            object_list.append(attachment["objectKey"])
        if object_list:
            await self.delete_azure_blobs_api(object_list)
        result =  self.attachments.delete_many(variables).deleted_count
        return result > 0

    async def delete_attachment_api(self, id: str) -> bool:
        """
        delete one attachment
        delete parms: {id} - attachment id
        """
        variables = {"id": id}
        attachment =  self.attachments.find_one(variables)
        if not attachment:
            return False
        if attachment.get("objectKey"):
            await self.delete_azure_blobs_api([attachment["objectKey"]])
        result =  self.attachments.delete_one(variables).deleted_count
        return result > 0
    
#endregion

#region Step API
    async def create_steps_api(self, steps: List[Union[StepDict, "Step"]]) -> "Dict":
        """responsible for only create new steps"""
        step_datas: List[StepDict] = [step.to_dict() if isinstance(step, Step) else step for step in steps]
        self.steps.insert_many(step_datas)
        return {"ok": True, "message": "Steps ingested successfully"}
    

    async def update_step_api(self, step: "StepDict") -> "Dict":
        """
        id: Optional[str]
        name: Optional[str]
        type: Optional[StepType]
        threadId: Optional[str]
        error: Optional[str]
        input: Optional[Dict]
        output: Optional[Dict]
        metadata: Optional[Dict]
        tags: Optional[List[str]]
        parentId: Optional[str]
        createdAt: Optional[str]
        startTime: Optional[str]
        endTime: Optional[str]
        generation: Optional[Dict]
        scores: Optional[List[ScoreDict]]
        attachments: Optional[List[AttachmentDict]]
        """
        
        step_id = step["id"]
        if not step_id:
            raise ValueError("Step ID is required")
        update_params = {}
        update_params["output.content"] = step["output"]
        update_params["createdAt"] = step["createdAt"]
        update_params["startTime"] = step["start"]
        update_params["endTime"] = step["end"]
        result = self.steps.update_one({"id": step_id}, {"$set": update_params})
        if not result:
             return {"ok": False, "message": "Failed to update step"}
        return {"ok": True, "message": "Step update successfully"}
    
    async def delete_step(self, step_id: str):
        self.steps.delete_one({"id": step_id})

#endregion

#region Thread api

    async def create_thread_api(
        self,
        name: Optional[str] = None,
        metadata: Optional[Dict] = None,
        participant_id: Optional[str] = None,
        environment: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Optional[Thread]:
        metadata = metadata or {}
        variables = {
            "name": name,
            "metadata": metadata,
            "participantId": participant_id,
            "environment": environment,
            "tags": tags,
        }

        oid = await self.threads.insert_one(variables).inserted_id

        if not oid:
            return None
        #logger.info("Thread created")
        thread = self.threads.find_one({"_id": ObjectId(oid)})

        return Thread.from_dict(thread)

    async def delete_thread_api(self, id: str) ->bool:
        result_delete_feedbacks = self.feedbacks.delete_many({"threadId": id}).deleted_count
        #logger.info("Deleted feedbacks: %r", result_delete_feedbacks)    

        result_delete_attachments = await self.delete_attachments_api(threadId=id)
        #logger.info("Deleted attachments: %r", result_delete_attachments)
        from .embedding_helper import adel_collection
        #result_delete_vectordb = await adel_collection(thread_id=id)

        result_steps_delete = self.steps.delete_many({"threadId": id}).deleted_count
        #logger.info("Deleted steps: %r", result_steps_delete)
        
        result_delete_thread = self.threads.delete_one({"id": id}).deleted_count
        #logger.info("Deleted thread: %r", result_delete_thread)
        
        return result_steps_delete >0 and result_delete_thread > 0 and result_delete_attachments and result_delete_feedbacks > 0 


    
    async def list_threads_api(
        self,
        first: Optional[int] = None,
        after: Optional[str] = None,
        filters: Optional[ThreadFilter] = None,
    ) -> PaginatedResponse["Thread"]:
        """
        input parms:
        {
            first: Optional[int] = None,
            after: Optional[str] = None,
            filters: Optional[ThreadFilter] = None,
        }
        output parms:
        {
            data: List[Thread],
            pageInfo: PageInfo,
        }"""

        if not filters.userId:
            raise ValueError("participantsIdentifier is required")
        
        
        threads_by_user_filter = {}
        threads_by_user_filter["participant.id"] =  filters.userId

        thread_by_participant = self.threads.find(threads_by_user_filter, {"id": 1, "_id":0}).sort("createdAt", DESCENDING)
        _thread_ids = list(thread_by_participant)
        final_threadId_query = {}
        and_id_query = []
        if _thread_ids:
            and_id_query= [m["id"] for m in _thread_ids]
       
        # search query in steps with parentId = None and threadId in _thread_ids   
        if filters.search:
            search_query = {}
            search_query["$text"] = {"$search": filters.search,"$caseSensitive": False}
            search_query["parentId"] = None
            search_query["threadId"] = {"$in": and_id_query}
            search_thread_ids =  self.steps.distinct(key="threadId",filter=search_query)
            if search_thread_ids:
                and_id_query.clear()
                and_id_query = search_thread_ids
            else: #if no result found
                and_id_query.clear()
        
        if filters.feedback is not None: #filter feedback can be 0 or 1-> cannot use if filters.feedback
            logger.info("Filtering by feedback %r", filters.feedback)
            feedback_query = {}
            feedback_query["value"] = filters.feedback
            feedback_query["threadId"] = {"$in": and_id_query}
            
            # feedback_query["feedbacks"] = { "$exists": True }
            list_threadids = self.feedbacks.distinct(key="threadId",filter=feedback_query)
            
            if list_threadids:  
                and_id_query.clear()
                and_id_query = list_threadids
            else:
                and_id_query.clear()
                    

                
        
        limit = first or 10  # Default limit of 10 if 'first' is not provided
        
        # skip = 0
        if after:
        # Fetch the document corresponding to the 'after' ID to get its 'createdAt' value
            after_doc = self.threads.find_one({"id": after}, {"createdAt": 1, "_id": 0})
            if after_doc:
                # Use the 'createdAt' value of the 'after' document for pagination
                final_threadId_query["createdAt"] = {"$lt": after_doc["createdAt"]}

        # Build the query
        final_threadId_query["id"] = {"$in": and_id_query}
        cursor = self.threads.find(final_threadId_query, {"_id": 0}).limit(limit + 1).sort("createdAt", DESCENDING)
    
        threads = list(cursor)
        # logger.info("Threads lenght: %r, after: %r", len(threads), after)
        has_next_page = len(threads) > limit
        if has_next_page:
            threads.pop()  # Remove the extra document to match 'limit'
        
        response = {"data": threads, 
                    "pageInfo": 
                        {"hasNextPage": has_next_page, "endCursor": (threads[-1]['id']) if threads else None}
                    }

        return PaginatedResponse[Thread].from_dict(response, Thread)
                                               
    
    def convert_object_id_to_str(self,data):
        if isinstance(data, dict):
            for key, value in data.items():
                if key == '_id':
                    data[key] = str(value)  # Convert ObjectId to string
                else:
                    self.convert_object_id_to_str(value)  # Recurse for nested structures
        elif isinstance(data, list):
            for item in data:
                self.convert_object_id_to_str(item)
    
    async def get_thread_api(self, id: str) -> Optional[Thread]:
    
        query = {"id": id}
        thread = self.threads.find_one(query,{"_id":0})
        if not thread:
            return None
        steps = list(self.steps.find({"threadId": id}).sort("createdAt", ASCENDING))

        feedbacks = list(self.feedbacks.find({"threadId": id},{"_id":0,"threadId":0}))#type: list[ScoreDict]
        
        attachments = list(self.attachments.find({"threadId": id},{"_id":0,"threadId":0 }))#type: list[AttachmentDict]
        for step in steps:
            _feedbacks = [feedback for feedback in feedbacks if feedback["stepId"] == step["id"]] 
            _attachments = [attachment for attachment in attachments if attachment["stepId"] == step["id"]]
            if _feedbacks:
                
                step["scores"] = _feedbacks
                
            if _attachments:
                step["attachments"] = _attachments

        thread["steps"] = steps
        return Thread.from_dict(thread)

    async def get_user_from_threadid_api(self, thread_id: str) -> Optional[User]:
        thread = self.threads.find_one({"id": thread_id})
        if not thread:
            return None
        if not thread.get("participant"):
            return None
        user = thread.get("participant")
        if not user:
            return None
        return User.from_dict(user) #no metadata

    async def upsert_thread_api(
        self,
        id: str,
        name: Optional[str] = None,
        metadata: Optional[Dict] = None,
        participant: Optional[UserDict] = None,
        environment: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Thread:
        #logger.info("Upserting thread metadata")
        result = self.threads.update_one({"id": id}, 
                                         {"$set": 
                                            {
                                                "id": id,
                                                "metadata": metadata, 
                                                "participant": participant, 
                                                "environment": environment, 
                                                "tags": tags
                                             },
                                            "$setOnInsert": {"createdAt": datetime.datetime.now().isoformat(), "name": name}
                                        }
                                        ,upsert=True
                                        ).upserted_id
        
        thread = self.threads.find_one({"_id":ObjectId(result)}) 
        if not thread:
            return 
        return Thread.from_dict(thread)
#endregion

# region Upload to azure blob storage api
    
    CONTAINER_NAME = "attachments"
    # from azure.identity.aio import DefaultAzureCredential
    from azure.storage.blob.aio import BlobServiceClient, BlobClient, ContainerClient

    async def upload_file_api(
        self,
        content: Union[bytes, str],
        thread_id: str,
        mime: Optional[str] = "application/octet-stream",
    ) -> Dict:
        """
        Uploads a file to the server and returns the object key and signed URL for the file.
        input parms:
        {
            content: Union[bytes, str],
            thread_id: str,
            mime: Optional[str] = "application/octet-stream",
        }
        output parms:
        {
            object_key: Optional[str],
            url: Optional[str],
        }
        """
        assert mime is not None, "MIME type is required"
        id = str(uuid.uuid4())
        
        blob_path = f"{thread_id}/{id}"
        logger.info("Uploading file: %r", blob_path)
        metadata = {"fileName": id, "contentType": mime, "threadId": thread_id}
        import os
        from azure.storage.blob import BlobServiceClient 
        account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
        credential = os.getenv("AZURE_STORAGE_ACCESS_KEY")
        
       
        with BlobServiceClient(account_url=account_url, credential=credential) as client:
            blob_service_client:BlobServiceClient = client
            container_client = blob_service_client.get_container_client(container=self.CONTAINER_NAME)
            if not container_client.exists():
                container_client.create_container()
            blob_client = container_client.get_blob_client(blob_path)   
            try:
                
                blob_client.upload_blob(data=content, overwrite=True, metadata=metadata, content_settings=ContentSettings(content_type=mime))
                signed_url = self.create_service_sas_blob_url(blob_client=blob_client, account_key=credential,)
                #logger.info("File uploaded: %r", blob_path)
                return {"object_key": blob_path, "url": signed_url}
            except Exception as e:
                logger.error(f"Failed to upload file: {str(e)}")
                return {"object_key": None, "url": None} 
        
    def create_service_sas_blob_url(self,blob_client: BlobClient, account_key: str) -> str:
    # Create a SAS token that's valid for 30 day

        start_time = datetime.datetime.now(datetime.timezone.utc)
        expiry_time = start_time + datetime.timedelta(days=30)

        sas_token = generate_blob_sas(
            account_name=blob_client.account_name,
            container_name=blob_client.container_name,
            blob_name=blob_client.blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry_time,
            start=start_time
        )
        return f"{blob_client.url}?{sas_token}"

    async def delete_azure_blob_api(self, object_key:List[str]) -> bool:
        """
        Deletes a file from the server.
        input parms:
        {
            object_key: str,
        }
        """
        import os
        from azure.storage.blob import BlobServiceClient 
        account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
        credential = os.getenv("AZURE_STORAGE_ACCESS_KEY")
        with BlobServiceClient(account_url=account_url, credential=credential) as client:
            blob_service_client:BlobServiceClient = client
            container_client = blob_service_client.get_container_client(container=self.CONTAINER_NAME)
            for key in object_key:
                blob_client = container_client.get_blob_client(key)
                try:
                    await blob_client.delete_blob()
                except Exception as e:
                    logger.error(f"Failed to delete file: {str(e)}")
                    return False
                
    async def delete_azure_blobs_api(self,object_key:list[str]) -> bool:
        import os
        from azure.storage.blob import BlobServiceClient
        account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
        credential = os.getenv("AZURE_STORAGE_ACCESS_KEY")
        
        with BlobServiceClient(account_url=account_url, credential=credential) as client:
            blob_service_client:BlobServiceClient = client
            container_client = blob_service_client.get_container_client(container=self.CONTAINER_NAME)
            try:
                container_client.delete_blobs(*object_key)
            except Exception as e:
                logger.error(f"Failed to delete file: {str(e)}")
                return False

#endregion
