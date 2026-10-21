/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import { useFormContext } from 'react-hook-form'
import { useTranslation } from 'react-i18next'

import {
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'

import { SettingsCard } from '../components/settings-card'
import { safeNumberFieldProps } from '../utils/numeric-field'
import type { RoutingPolicyFormValues } from './routing-form'

export function RetrySection() {
  const { t } = useTranslation()
  const form = useFormContext<RoutingPolicyFormValues>()
  return (
    <SettingsCard title={t('Retry budget')} className='shadow-none'>
      <div className='space-y-4'>
        <FormField
          control={form.control}
          name='RetryTimes'
          render={({ field }) => (
            <FormItem>
              <FormLabel>{t('Maximum retries')}</FormLabel>
              <FormControl>
                <Input
                  type='number'
                  min={0}
                  max={99}
                  step={1}
                  {...safeNumberFieldProps(field)}
                />
              </FormControl>
              <FormDescription>
                {t('Excludes the first attempt. Counted per group.')}
              </FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name='AutomaticRetryStatusCodes'
          render={({ field }) => (
            <FormItem>
              <FormLabel>{t('Auto-retry status codes')}</FormLabel>
              <FormControl>
                <Input
                  {...field}
                  placeholder={t('e.g. 401, 403, 429, 500-599')}
                />
              </FormControl>
              <FormDescription>
                {t('2xx, 504 and 524 are always excluded.')}
              </FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />
        <div className='border-t pt-4'>
          <h3 className='text-sm font-medium'>{t('Native task retries')}</h3>
          <p className='text-sm text-muted-foreground'>
            {t('Applies to newly accepted tasks; existing tasks keep their policy.')}
          </p>
        </div>
        <div className='grid gap-4 sm:grid-cols-2'>
          {([
            ['DurableTaskAttemptBudget', 'Supplier attempt budget', 1, 8],
            ['DurableTaskSuccessorDelaySeconds', 'Safe successor delay (seconds)', 1, 60],
            ['DurableTaskReconcileDelaySeconds', 'Receipt reconciliation delay (seconds)', 1, 3600],
            ['DurableTaskReplayInitialDelaySeconds', 'First same-key replay delay (seconds)', 30, 3600],
            ['DurableTaskReplayMaxDelaySeconds', 'Maximum same-key replay delay (seconds)', 30, 86400],
            ['DurableTaskReplayJitterSeconds', 'Same-key replay jitter (seconds)', 0, 60],
          ] as const).map(([name, label, min, max]) => (
            <FormField
              key={name}
              control={form.control}
              name={name}
              render={({ field }) => (
                <FormItem>
                  <FormLabel>{t(label)}</FormLabel>
                  <FormControl>
                    <Input type='number' min={min} max={max} step={1} {...safeNumberFieldProps(field)} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
          ))}
        </div>
        <FormField
          control={form.control}
          name='DurableTaskUncertainRetryEnabled'
          render={({ field }) => (
            <FormItem className='flex items-center justify-between gap-4 border-t pt-4'>
              <div>
                <FormLabel>{t('Allow one uncertain task replacement')}</FormLabel>
                <FormDescription>{t('Off by default and applies only to new tasks. May create a duplicate supplier generation at operator expense; the client is charged once per task.')}</FormDescription>
              </div>
              <FormControl><Switch checked={field.value} onCheckedChange={field.onChange} /></FormControl>
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name='DurableTaskUncertainRetryDelaySeconds'
          render={({ field }) => (
            <FormItem>
              <FormLabel>{t('Uncertain replacement delay (seconds)')}</FormLabel>
              <FormControl><Input type='number' min={60} max={3600} step={1} {...safeNumberFieldProps(field)} /></FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
      </div>
    </SettingsCard>
  )
}
