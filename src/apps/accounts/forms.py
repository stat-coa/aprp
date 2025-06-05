from django import forms
from django.core.validators import RegexValidator
from django.contrib.auth import authenticate, get_user_model
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth.password_validation import validate_password
from django.utils.safestring import mark_safe
from django.utils.translation import ugettext as _

from . import models


User = get_user_model()
groups = models.GroupInformation.objects.end_groups()
chValidator = RegexValidator(
    '[^0-9a-zA-Z]',
    _('Please Input Chinese Only')
)


class UserEditForm(forms.ModelForm):
    username = forms.CharField(
        label=_('Account')
    )
    first_name = forms.CharField(
        label=_('First Name'),
        validators=[chValidator]
    )
    last_name = forms.CharField(
        label=_('Last Name'),
        validators=[chValidator]
    )
    group = forms.ModelChoiceField(
        queryset=groups,
        label=_('Group'),
        empty_label=_('Choose one of the following...')
    )
    reset_email = forms.EmailField(
        label=_('Reset Email'),
        required=False
    )

    def __init__(self, *args, **kwargs):
        super(UserEditForm, self).__init__(*args, **kwargs)
        self.fields['username'].widget.attrs['readonly'] = True
        self.fields['email'].widget.attrs['readonly'] = True

        if hasattr(self, 'instance'):
            for group in self.instance.groups.all():
                if not group.info.has_child:
                    self.fields['group'].initial = group.info.id

    class Meta:
        model = User
        fields = [
            'username', 'email', 'reset_email',
            'first_name', 'last_name', 'group'
        ]

    def clean(self, *args, **kwargs):
        user = self.instance
        group = self.cleaned_data.get('group')
        email = self.cleaned_data.get('email')
        reset_email = self.cleaned_data.get('reset_email')

        if reset_email != '':
            if reset_email != user.email:
                email_qs = User.objects.filter(
                    email=reset_email
                )

                if email_qs.exists():
                    self.add_error(
                        'reset_email',
                        _('This email has already been registered')
                    )
                else:
                    models.ResetEmailProfile.objects.create(
                        user=user,
                        new_email=reset_email
                    )

        else:
            dns = email.split('@')[1]

            if dns != group.email_dns:
                self.add_error(
                    'group',
                    (
                            _(
                                'To use this group, please change your email to this unit:'
                            ) + group.name
                    )
                )

        return super(UserEditForm, self).clean()

    def save(self, commit=True):
        form = super(UserEditForm, self).save(commit=False)

        if commit:
            user = self.instance
            group_info = self.cleaned_data.get('group')

            # Remove previous groups and assign to new groups
            for info in models.GroupInformation.objects.all():
                if info in group_info.parents():
                    info.group.user_set.add(user)
                else:
                    info.group.user_set.remove(user)

            form.save()

        return form


class UserLoginForm(forms.Form):
    username = forms.CharField(
        label=_('Account'),
        help_text=_('Please enter your account')
    )
    password = forms.CharField(
        widget=forms.PasswordInput,
        label=_('Password'),
        help_text=_('Please enter your password')
    )
    remember = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput()
    )
    # This field just for identity validate inactive error in views.py
    is_active = forms.BooleanField(
        widget=forms.HiddenInput,
        required=False,
        initial=True
    )

    def clean(self, *args, **kwargs):
        username = self.cleaned_data.get('username')
        password = self.cleaned_data.get('password')

        if username == 'guest':
            raise forms.ValidationError(
                _('Guest account will no longer accessible. Please feel free to register new account')
            )

        if username and password:
            user = authenticate(
                username=username,
                password=password
            )

            if not user:
                raise forms.ValidationError(
                    _('Your account or password is incorrect')
                )

            default_password = f'{user.username}{user.username}'

            if not user.is_active:
                if user.check_password(default_password):
                    return super().clean()

                self.cleaned_data['is_active'] = False
                raise forms.ValidationError(
                    mark_safe(_('Please activate your account'))
                )

        return super(UserLoginForm, self).clean()


class UserRegisterForm(forms.ModelForm):
    username = forms.CharField(
        label=_('Account'),
        help_text=_('Please enter your account')
    )
    email = forms.EmailField(
        label=_('Email'),
        help_text=_('Please enter your email')
    )
    first_name = forms.CharField(
        label=_('First Name'),
        validators=[chValidator]
    )
    last_name = forms.CharField(
        label=_('Last Name'),
        validators=[chValidator]
    )
    password = forms.CharField(
        widget=forms.PasswordInput,
        label=_('Password'),
        help_text=_('Please enter your password')
    )
    password2 = forms.CharField(
        widget=forms.PasswordInput,
        label=_('Confirm Password'),
        help_text=_('Please confirm your password')
    )
    condition = forms.BooleanField(
        required=True,
        widget=forms.CheckboxInput(),
        label=_('Terms & Conditions')
    )

    class Meta:
        model = User
        fields = [
            'username',
            'email',
            'last_name',
            'first_name',
            'password',
            'password2',
            'condition',
        ]

    def clean_condition(self):
        condition = self.cleaned_data.get('condition')

        if not condition:
            raise forms.ValidationError(
                _('You must agree our terms and conditions')
            )

        return condition

    def clean_password(self):
        password = self.cleaned_data.get('password')
        validate_password(password)

        return password

    def clean_password2(self):
        password = self.cleaned_data.get('password')
        password2 = self.cleaned_data.get('password2')

        if password != password2:
            raise forms.ValidationError(_('Password must match'))

        return password

    def clean_email(self):
        email = self.cleaned_data.get('email')

        # restrict email dns
        dns = email.split('@')[1]
        if not models.GroupInformation.objects.filter(email_dns=dns).count():
            raise forms.ValidationError(
                _('Please register with email under domain "@mail.moa.gov.tw"')
            )

        email_qs = User.objects.filter(email=email)

        if email_qs.exists():
            raise forms.ValidationError(
                _('This email has already been registered')
            )

        return email


class UserResetPasswordForm(forms.Form):
    password = forms.CharField(
        widget=forms.PasswordInput,
        required=True,
        label=_('Password'),
        help_text=_('Please enter your password')
    )
    password2 = forms.CharField(
        widget=forms.PasswordInput,
        required=True,
        label=_('Confirm Password'),
        help_text=_('Please confirm your password')
    )

    class Meta:
        model = User
        fields = [
            'password',
            'password2'
        ]

    def clean_password(self):
        password = self.cleaned_data.get('password')
        validate_password(password)

        return password

    def clean_password2(self):
        password = self.cleaned_data.get('password')
        password2 = self.cleaned_data.get('password2')

        if password != password2:
            raise forms.ValidationError(
                _('Password must match')
            )

        return password


class ResendEmailForm(forms.Form):
    email = forms.EmailField(
        required=False,
        label=_('Email'),
        help_text=_('Please enter your email')
    )
    username = forms.CharField(
        required=False,
        label=_('Account'),
        help_text=_('Please enter your account')
    )

    class Meta:
        model = User
        fields = [
            'email',
            'username',
        ]

    def clean(self):
        email = self.cleaned_data.get('email')
        username = self.cleaned_data.get('username')

        if email == '' and username == '':
            raise forms.ValidationError(
                _('Please advise your email or account')
            )

        if email and username:
            user = User.objects.filter(
                email=email,
                username=username
            ).first()

            if not user:
                raise forms.ValidationError(
                    _('Your account or email is incorrect')
                )

        return self.cleaned_data

    def clean_email(self):
        email = self.cleaned_data.get('email')

        if email:
            email_qs = User.objects.filter(email=email)

            if not email_qs.exists():
                raise forms.ValidationError(
                    _('This email is not exist')
                )

        return email

    def clean_username(self):
        username = self.cleaned_data.get('username')

        if username:
            username_qs = User.objects.filter(
                username=username
            )

            if not username_qs.exists():
                raise forms.ValidationError(
                    _('This account is not exist')
                )

        return username


class ChangePasswordForm(PasswordChangeForm):
    old_password = forms.CharField(
        label=_('目前密碼'),
        widget=forms.PasswordInput,
        help_text=_('請輸入您目前正在使用的密碼。')
    )
    new_password1 = forms.CharField(
        label=_('新密碼'),
        widget=forms.PasswordInput,
        help_text=_('請輸入新的密碼，最少為 8 個字元。')
    )
    new_password2 = forms.CharField(
        label=_('確認新密碼'),
        widget=forms.PasswordInput,
        help_text=_('再次輸入新密碼以確認。')
    )

    def __init__(self, user, *args, **kwargs):
        super().__init__(user, *args, **kwargs)
        self.user = user

    def clean(self):
        default_password = f'{self.user.username}{self.user.username}'

        if not self.user.check_password(default_password):
            raise forms.ValidationError(_('請使用新密碼登入'))

        return super().clean()

    def save(self, commit=True):
        user = super().save(commit=commit)
        info = user.info
        info.reporter = True
        info.save()

        if not user.is_active:
            user.is_active = True
            user.save()

        return user
