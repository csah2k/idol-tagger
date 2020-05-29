import json
import pkg_resources

from django_admin_client import DjangoAdminDynamic, DjangoAdminModel


class ModelBase:
    def __init__(self, model: DjangoAdminModel):
        self._model = model

    def all(self):
        return self._model.all()

    def find(self, query: str):
        return self._model.find(query)

    def add(self, **kwargs):
        return self._model.add(kwargs)

    def get(self, object_id: str):
        return self._model.get(object_id)

    def delete(self, object_id: str):
        return self._model.delete(object_id)


class ApiDocumentAnnotationsModel(ModelBase):
    def add(
        self,
        prob: str,
        user: str,
        document: str,
        label: str,
        manual: str = ''
    ):
        return self._model.add({
            'prob': prob,
            'manual': manual,
            'user': user,
            'document': document,
            'label': label
        })


class ApiDocumentsModel(ModelBase):
    def add(
        self,
        text: str,
        project: str,
        meta: str,
        annotations_approved_by: str
    ):
        return self._model.add({
            'text': text,
            'project': project,
            'meta': meta,
            'annotations_approved_by': annotations_approved_by
        })


class ApiLabelsModel(ModelBase):
    def add(
        self,
        text: str,
        project: str,
        background_color: str,
        text_color: str,
        prefix_key: str = '',
        suffix_key: str = ''
    ):
        return self._model.add({
            'text': text,
            'prefix_key': prefix_key,
            'suffix_key': suffix_key,
            'project': project,
            'background_color': background_color,
            'text_color': text_color
        })


class ApiProjectsModel(ModelBase):
    def add(
        self,
        name: str,
        description: str,
        guideline: str,
        users: str,
        project_type: str,
        randomize_document_order: str = '',
        collaborative_annotation: str = ''
    ):
        return self._model.add({
            'name': name,
            'description': description,
            'guideline': guideline,
            'users': users,
            'project_type': project_type,
            'randomize_document_order': randomize_document_order,
            'collaborative_annotation': collaborative_annotation
        })


class ApiRoleMappingsModel(ModelBase):
    def add(
        self,
        user: str,
        project: str,
        role: str
    ):
        return self._model.add({
            'user': user,
            'project': project,
            'role': role
        })


class ApiRolesModel(ModelBase):
    def add(
        self,
        name: str,
        description: str
    ):
        return self._model.add({
            'name': name,
            'description': description
        })


class ApiSeq2seqAnnotationsModel(ModelBase):
    def add(
        self,
        prob: str,
        user: str,
        document: str,
        text: str,
        manual: str = ''
    ):
        return self._model.add({
            'prob': prob,
            'manual': manual,
            'user': user,
            'document': document,
            'text': text
        })


class ApiSeq2seqProjectsModel(ModelBase):
    def add(
        self,
        name: str,
        description: str,
        guideline: str,
        users: str,
        project_type: str,
        randomize_document_order: str = '',
        collaborative_annotation: str = ''
    ):
        return self._model.add({
            'name': name,
            'description': description,
            'guideline': guideline,
            'users': users,
            'project_type': project_type,
            'randomize_document_order': randomize_document_order,
            'collaborative_annotation': collaborative_annotation
        })


class ApiSequenceAnnotationsModel(ModelBase):
    def add(
        self,
        prob: str,
        user: str,
        document: str,
        label: str,
        start_offset: str,
        end_offset: str,
        manual: str = ''
    ):
        return self._model.add({
            'prob': prob,
            'manual': manual,
            'user': user,
            'document': document,
            'label': label,
            'start_offset': start_offset,
            'end_offset': end_offset
        })


class ApiSequenceLabelingProjectsModel(ModelBase):
    def add(
        self,
        name: str,
        description: str,
        guideline: str,
        users: str,
        project_type: str,
        randomize_document_order: str = '',
        collaborative_annotation: str = ''
    ):
        return self._model.add({
            'name': name,
            'description': description,
            'guideline': guideline,
            'users': users,
            'project_type': project_type,
            'randomize_document_order': randomize_document_order,
            'collaborative_annotation': collaborative_annotation
        })


class ApiTextClassificationProjectsModel(ModelBase):
    def add(
        self,
        name: str,
        description: str,
        guideline: str,
        users: str,
        project_type: str,
        randomize_document_order: str = '',
        collaborative_annotation: str = ''
    ):
        return self._model.add({
            'name': name,
            'description': description,
            'guideline': guideline,
            'users': users,
            'project_type': project_type,
            'randomize_document_order': randomize_document_order,
            'collaborative_annotation': collaborative_annotation
        })


class AuthtokenTokensModel(ModelBase):
    def add(
        self,
        user: str
    ):
        return self._model.add({
            'user': user
        })


class AuthGroupsModel(ModelBase):
    def add(
        self,
        name: str,
        permissions: str = ''
    ):
        return self._model.add({
            'name': name,
            'permissions': permissions
        })


class AuthUsersModel(ModelBase):
    def add(
        self,
        username: str,
        password1: str,
        password2: str
    ):
        return self._model.add({
            'username': username,
            'password1': password1,
            'password2': password2
        })


class SocialDjangoAssociationsModel(ModelBase):
    def add(
        self,
        server_url: str,
        handle: str,
        secret: str,
        issued: str,
        lifetime: str,
        assoc_type: str
    ):
        return self._model.add({
            'server_url': server_url,
            'handle': handle,
            'secret': secret,
            'issued': issued,
            'lifetime': lifetime,
            'assoc_type': assoc_type
        })


class SocialDjangoNoncesModel(ModelBase):
    def add(
        self,
        server_url: str,
        timestamp: str,
        salt: str
    ):
        return self._model.add({
            'server_url': server_url,
            'timestamp': timestamp,
            'salt': salt
        })


class SocialDjangoUserSocialAuthsModel(ModelBase):
    def add(
        self,
        user: str,
        provider: str,
        uid: str,
        extra_data: str
    ):
        return self._model.add({
            'user': user,
            'provider': provider,
            'uid': uid,
            'extra_data': extra_data
        })


class ApiApp:
    def __init__(self, da):
        self.document_annotations = ApiDocumentAnnotationsModel(da.document_annotations)
        self.documents = ApiDocumentsModel(da.documents)
        self.labels = ApiLabelsModel(da.labels)
        self.projects = ApiProjectsModel(da.projects)
        self.role_mappings = ApiRoleMappingsModel(da.role_mappings)
        self.roles = ApiRolesModel(da.roles)
        self.seq2seq_annotations = ApiSeq2seqAnnotationsModel(da.seq2seq_annotations)
        self.seq2seq_projects = ApiSeq2seqProjectsModel(da.seq2seq_projects)
        self.sequence_annotations = ApiSequenceAnnotationsModel(da.sequence_annotations)
        self.sequence_labeling_projects = ApiSequenceLabelingProjectsModel(da.sequence_labeling_projects)
        self.text_classification_projects = ApiTextClassificationProjectsModel(da.text_classification_projects)


class AuthtokenApp:
    def __init__(self, da):
        self.tokens = AuthtokenTokensModel(da.tokens)


class AuthApp:
    def __init__(self, da):
        self.groups = AuthGroupsModel(da.groups)
        self.users = AuthUsersModel(da.users)


class SocialDjangoApp:
    def __init__(self, da):
        self.associations = SocialDjangoAssociationsModel(da.associations)
        self.nonces = SocialDjangoNoncesModel(da.nonces)
        self.user_social_auths = SocialDjangoUserSocialAuthsModel(da.user_social_auths)


class DoccanoDjangoAdminClient:
    def __init__(self, base_url, superuser_email, superuser_password):

        package = 'doccano_admin_client'
        path = 'spec.json'
        f = pkg_resources.resource_stream(package, path)
        spec = json.load(f)

        self._da = DjangoAdminDynamic.from_spec(
            base_url=base_url,
            superuser_email=superuser_email,
            superuser_password=superuser_password,
            spec=spec
        )

        self.social_django = SocialDjangoApp(self._da)
        self.api = ApiApp(self._da)
        self.auth = AuthApp(self._da)
        self.authtoken = AuthtokenApp(self._da)
