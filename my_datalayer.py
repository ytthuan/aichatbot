import json
from venv import logger
from literalai import LiteralClient, Score as LiteralScore, Step as LiteralStep, Thread as LiteralThread, Attachment as LiteralAttachment
import chainlit.data as cl_data

#chainlit 1.1.306
from literalai.observability.step import StepDict as LiteralStepDict #chainlit 1.3.0rc1
from chainlit.element import Audio, Element, ElementDict, File, Image, Pdf, Text, Video
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union, cast
from chainlit import logger as log


if TYPE_CHECKING:
    from chainlit.element import Element, ElementDict
    from chainlit.step import FeedbackDict, StepDict
    from chainlit.user import PersistedUser, User

from chainlit.step import (
    FeedbackDict,
    Step,
    StepDict,
    StepType,
    TrueStepType,
    check_add_step_in_cot,
    stub_step,
)

from chainlit.types import (
    Feedback,
    PaginatedResponse,
    Pagination,
    ThreadDict,
    ThreadFilter,
)

from chainlit.types import Feedback, Pagination, ThreadDict, ThreadFilter
from chainlit.user import PersistedUser, User
import aiofiles
from .mongodb_api import MONGODB_API #import your proper mongodb api code
from os import getenv


class LiteralToChainlitConverter:
    @classmethod
    def steptype_to_steptype(cls, step_type: Optional[StepType]) -> TrueStepType:
        if step_type in ["user_message", "assistant_message", "system_message"]:
            return "undefined"
        return cast(TrueStepType, step_type or "undefined")

    @classmethod
    def score_to_feedbackdict(
        cls,
        score: Optional[LiteralScore],
    ) -> "Optional[FeedbackDict]":
        if not score:
            return None
        return {
            "id": score.id or "",
            "forId": score.step_id or "",
            "value": cast(Literal[0, 1], score.value),
            "comment": score.comment,
        }

    @classmethod
    def step_to_stepdict(cls, step: LiteralStep) -> "StepDict":
        metadata = step.metadata or {}
        input = (step.input or {}).get("content") or (
            json.dumps(step.input) if step.input and step.input != {} else ""
        )
        output = (step.output or {}).get("content") or (
            json.dumps(step.output) if step.output and step.output != {} else ""
        )

        user_feedback = (
            next(
                (
                    s
                    for s in step.scores
                    if s.type == "HUMAN" and s.name == "user-feedback"
                ),
                None,
            )
            if step.scores
            else None
        )

        return {
            "createdAt": step.created_at,
            "id": step.id or "",
            "threadId": step.thread_id or "",
            "parentId": step.parent_id,
            "feedback": cls.score_to_feedbackdict(user_feedback),
            "start": step.start_time,
            "end": step.end_time,
            "type": step.type or "undefined",
            "name": step.name or "",
            "generation": step.generation.to_dict() if step.generation else None,
            "input": input,
            "output": output,
            "showInput": metadata.get("showInput", False),
            "indent": metadata.get("indent"),
            "language": metadata.get("language"),
            "isError": bool(step.error),
            "waitForAnswer": metadata.get("waitForAnswer", False),
        }

    @classmethod
    def attachment_to_elementdict(cls, attachment: LiteralAttachment) -> ElementDict:
        metadata = attachment.metadata or {}
        return {
            "chainlitKey": None,
            "display": metadata.get("display", "side"),
            "language": metadata.get("language"),
            "autoPlay": metadata.get("autoPlay", None),
            "playerConfig": metadata.get("playerConfig", None),
            "page": metadata.get("page"),
            "size": metadata.get("size"),
            "type": metadata.get("type", "file"),
            "forId": attachment.step_id,
            "id": attachment.id or "",
            "mime": attachment.mime,
            "name": attachment.name or "",
            "objectKey": attachment.object_key,
            "url": attachment.url,
            "threadId": attachment.thread_id,
        }

    @classmethod
    def attachment_to_element(
        cls, attachment: LiteralAttachment, thread_id: Optional[str] = None
    ) -> Element:
        metadata = attachment.metadata or {}
        element_type = metadata.get("type", "file")

        element_class = {
            "file": File,
            "image": Image,
            "audio": Audio,
            "video": Video,
            "text": Text,
            "pdf": Pdf,
        }.get(element_type, Element)

        assert thread_id or attachment.thread_id

        element = element_class(
            name=attachment.name or "",
            display=metadata.get("display", "side"),
            language=metadata.get("language"),
            size=metadata.get("size"),
            url=attachment.url,
            mime=attachment.mime,
            thread_id=thread_id or attachment.thread_id,
        )
        element.id = attachment.id or ""
        element.for_id = attachment.step_id
        element.object_key = attachment.object_key
        return element

    @classmethod
    def step_to_step(cls, step: LiteralStep) -> Step:
        chainlit_step = Step(
            name=step.name or "",
            type=cls.steptype_to_steptype(step.type),
            id=step.id,
            parent_id=step.parent_id,
            thread_id=step.thread_id or None,
        )
        chainlit_step.start = step.start_time
        chainlit_step.end = step.end_time
        chainlit_step.created_at = step.created_at
        chainlit_step.input = step.input.get("content", "") if step.input else ""
        chainlit_step.output = step.output.get("content", "") if step.output else ""
        chainlit_step.is_error = bool(step.error)
        chainlit_step.metadata = step.metadata or {}
        chainlit_step.tags = step.tags
        chainlit_step.generation = step.generation

        if step.attachments:
            chainlit_step.elements = [
                cls.attachment_to_element(attachment, chainlit_step.thread_id)
                for attachment in step.attachments
            ]

        return chainlit_step

    @classmethod
    def thread_to_threaddict(cls, thread: LiteralThread) -> ThreadDict:
        return {
            "id": thread.id,
            "createdAt": getattr(thread, "created_at", ""),
            "name": thread.name,
            "userId": thread.participant_id,
            "userIdentifier": thread.participant_identifier,
            "tags": thread.tags,
            "metadata": thread.metadata,
            "steps": [cls.step_to_stepdict(step) for step in thread.steps]
            if thread.steps
            else [],
            "elements": [
                cls.attachment_to_elementdict(attachment)
                for step in thread.steps
                for attachment in step.attachments
            ]
            if thread.steps
            else [],
        }


class MongoDbClient(LiteralClient):
    api: MONGODB_API
    def __init__(self, mongodb_uri: Optional[str] = None):
        self.api = MONGODB_API(mongodb_uri)


class MyMongoDbDataLayer(cl_data.BaseDataLayer):
    
    def __init__(self, mongodb_uri: Optional[str]):
        self.client = MongoDbClient(mongodb_uri=mongodb_uri)  # API key is unused
        log.info("Data layer initialized")
        

#region: implement the abstract methods for user related here
 
    async def get_user(self, identifier:str) -> Optional["PersistedUser"]:
        user = await self.client.api.get_user_api(identifier=identifier) 
        if not user:
            return None
        return PersistedUser(
            id=user.id or "",
            identifier=user.identifier or "",
            metadata=user.metadata,
            createdAt=user.created_at or "",
        )
        

    async def create_user(self, user: User) -> Optional["PersistedUser"]:
        _user = await self.client.api.get_user_api(identifier=user.identifier) 
        if not _user:# new user
            logger.info("user not found, creating new user")
            _key = ""
            if  user.metadata.get("role") == 'admin':
                _key = getenv("OPENAI_API_KEY")
            else:
                _key = await litellm.generate_key(user_identifier=user.identifier,premium_user=False)
            _user = await self.client.api.create_user_api(identifier=user.identifier, metadata={"key":_key,**user.metadata})
        
        elif _user: # existing user
            logger.info(f"user found updating user ")
            mt = {**_user.metadata,**user.metadata}
            if 'key' not in _user.metadata:
                _key = ""
                if  _user.metadata.get("role") == 'admin':
                    _key = getenv("OPENAI_API_KEY")
                else:
                    _key = await litellm.generate_key(user_identifier=user.identifier,premium_user=False)
                mt["key"] = _key
                logger.info(f"key generated for user {_user.identifier} : {_key}")
            await self.client.api.update_user_api(id=_user.id, identifier=user.identifier, metadata=mt)
        return PersistedUser(
            id=_user.id or "",
            identifier=_user.identifier or "",
            metadata=_user.metadata,
            createdAt=_user.created_at or "",
        )
    

#endregion    

#region: Implement the abstract methods for feedback related here
    async def upsert_feedback(
        self,
        feedback: Feedback,
    ) -> str:
        # feedback = self.client.api.get_feedback_api(feedback.id)
        if feedback.id:
            await self.client.api.update_feedback_api(
                id=feedback.id,
                update_params={
                    "comment": feedback.comment,
                    "value": feedback.value,
                },
            )
            return feedback.id
        else:
            created = await self.client.api.create_feedback_api(
                step_id=feedback.forId,
                value=feedback.value,
                comment=feedback.comment,
                name="user-feedback",
                type="HUMAN",
            )
            return created.id or ""
    async def delete_feedback(self,feedback_id: str):
        await self.client.api.delete_feedback_api(id=feedback_id)
        pass    
    
#endregion

#region: Implement the abstract methods for elements related here 

    cl_data.queue_until_user_message()
    async def create_element(self, element: "Element"):
        log.info("implemtation of create_element")
        """
        1. upload attachment to blob storage
        2. create attachment in MongoDB
        3. update step with attachment
        """
        
        metadata = {
            "size": element.size,
            "language": element.language,
            "display": element.display,
            "type": element.type,
            "page": getattr(element, "page", None),
            
        }
        # Implement the logic to create an element in the MongoDB database
        
        if not element.for_id:
            return
        object_key = None

        if not element.url:
            if element.path:
                async with aiofiles.open(element.path, "rb") as f:
                    content = await f.read()  # type: Union[bytes, str]
            elif element.content:
                content = element.content
            else:
                raise ValueError("Either path or content must be provided")
            uploaded = await self.client.api.upload_file_api(
                content=content, mime=element.mime, thread_id=element.thread_id
            )
            object_key = uploaded["object_key"]
            element.url = uploaded["url"]
            if not object_key:
                raise ValueError("Failed to upload file")
        
        # then create the attachment in the database
            await self.client.api.create_attachment_api(    id=element.id,
                                                            metadata=metadata, 
                                                            mime=element.mime, 
                                                            name=element.name, 
                                                            object_key=object_key, 
                                                            step_id=element.for_id, 
                                                            thread_id=element.thread_id, 
                                                            url=element.url )
            

    async def get_element(
        self, thread_id: str, element_id: str
    ) -> Optional["ElementDict"]:
        
        element = await self.client.api.get_attachment_api(id=element_id)
        return LiteralToChainlitConverter.attachment_to_elementdict(element)

    cl_data.queue_until_user_message()
    async def delete_element(self, element_id: str):
        await self.client.api.delete_attachment_api(id=element_id)

#endregion

#region Implement the abstract methods for steps related here
        
    @cl_data.queue_until_user_message()
    async def create_step(self, step_dict: "StepDict"):
        
        metadata = dict(
            step_dict.get("metadata", {}),
            **{
                # "disableFeedback": step_dict.get("disableFeedback"),
                # "isError": step_dict.get("isError"),
                "waitForAnswer": step_dict.get("waitForAnswer"),
                "language": step_dict.get("language"),
                "showInput": step_dict.get("showInput"),
            },
        )
        
        step: LiteralStepDict = {
            "createdAt": step_dict.get("createdAt"),
            "startTime": step_dict.get("start"),
            "endTime": step_dict.get("end"),
            "generation": step_dict.get("generation"),
            "id": step_dict.get("id"),
            "parentId": step_dict.get("parentId"),
            "name": step_dict.get("name"),
            "threadId": step_dict.get("threadId"),
            "type": step_dict.get("type"),
            "tags": step_dict.get("tags"),
            "metadata": metadata,
        }
        if step_dict.get("input"):
            step["input"] = {"content": step_dict.get("input")}
        if step_dict.get("output"):
            step["output"] = {"content": step_dict.get("output")}
        if step_dict.get("isError"):
            step["error"] = step_dict.get("output") 
        log.info("create_step")
        await self.client.api.create_steps_api([step])
#endregion

#region Implement the abstract methods for steps related here

    cl_data.queue_until_user_message()
    async def update_step(self, step_dict: "StepDict"):
        log.info("update_step")
        # Implement the logic to update a step in the MongoDB database
        await self.client.api.update_step_api(step_dict)

    cl_data.queue_until_user_message()
    async def delete_step(self, step_id: str):
        # Implement the logic to delete a step from the MongoDB database
        await self.client.api.delete_step(step_id)
#endregion

#region Implement the abstract methods for threads related here
    async def get_thread_author(self, thread_id: str) -> str: #chainlit version 1.3.0

        # Implement the logic to get the author of a thread from the MongoDB database
        thread = await self.get_thread(thread_id)
        if not thread:
            return ""
        user_identifier = thread.get("userIdentifier")
        if not user_identifier:
            return ""
        return user_identifier
    
    async def delete_thread(self, thread_id: str):
        log.info("implemtation of delete_thread")
        # Implement the logic to delete a thread from the MongoDB database
        result = await self.client.api.delete_thread_api(id=thread_id)
        return result
    
    async def list_threads(
        self, pagination: "Pagination", filters: "ThreadFilter"
    ) -> "PaginatedResponse[ThreadDict]":
        if not filters.userId:
            raise ValueError("userIdentifier is required")
        return await self.client.api.list_threads_api(
            first=pagination.first, after=pagination.cursor, filters=filters
        )

    async def get_thread(self, thread_id: str) -> "Optional[ThreadDict]":
        thread = await self.client.api.get_thread_api(id=thread_id)
        if thread is None:
            return None
        elements = []  # List[ElementDict]
        steps = []  # List[StepDict]
        if thread.steps:
            for step in thread.steps:
                if step.name =="Error":
                    continue
                
                for attachment in step.attachments:
                    elements.append(LiteralToChainlitConverter.attachment_to_elementdict(attachment))
                # comment for upgrading 1.1.301
                # if not config.features.prompt_playground and step.generation:
                #     step.generation = None
                chailit_step = LiteralToChainlitConverter.step_to_step(step)
                if check_add_step_in_cot(chailit_step):
                    steps.append(LiteralToChainlitConverter.step_to_stepdict(step))
                else: 
                    steps.append(stub_step(step))

        
        return {
            "createdAt": thread.created_at or "",
            "id": thread.id,
            "name": thread.name or None,
            "steps": steps,
            "elements": elements,
            "metadata": thread.metadata,
            "userId": thread.participant_id,
            "userIdentifier": thread.participant_identifier,
            "tags": thread.tags,
        }

    async def update_thread(
        self,
        thread_id: str,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
    ):
        if thread_id is None or thread_id == "":
            return
        user = None
        if not user_id: 
            user = await self.client.api.get_user_from_threadid_api(thread_id=thread_id)
        else:
            user = await self.client.api.get_user_api(id=user_id)
        if not user:    
            return
        metadata = metadata or {}
        if 'chat_history' in metadata:
            del metadata['chat_history']
            logger.info("chat_history in metadata deleted")
        logger.info("update or create thread")

        if memory := metadata.get("chat_settings", {}).get("memory"):
            await self.client.api.update_user_api(user.id, identifier=user.identifier, metadata={**user.metadata,"memory": memory,})
        if custom_instruction := metadata.get("instruction"):
            await self.client.api.update_user_api(user.id, identifier=user.identifier, metadata={**user.metadata,"instruction": custom_instruction})

        await self.client.api.upsert_thread_api(
                                                    id=thread_id, 
                                                    name=name, 
                                                    participant=user.to_dict(),
                                                    metadata=metadata, 
                                                    tags=tags,                                             
                                                    )
#endregion 
    

#region debug
    async def build_debug_url(self) -> str:
        pass
#endregion
