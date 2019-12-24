import { ElemID } from 'adapter-api'

export type SaltoErrorSeverity = 'Error' | 'Warning'

export type SaltoError = {
    message: string
    severity: SaltoErrorSeverity
}

export type SaltoElementError = SaltoError & {
    elemID: ElemID
}